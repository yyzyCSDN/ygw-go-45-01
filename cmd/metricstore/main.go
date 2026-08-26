package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"metricstore/internal/compact"
	"metricstore/internal/ingest"
	"metricstore/internal/metric"
	"metricstore/internal/model"
	"metricstore/internal/query"
	"metricstore/internal/retention"
	"metricstore/internal/series"
	"metricstore/internal/shard"
	"metricstore/internal/storage"
	"metricstore/internal/wal"
)

type server struct {
	ingest    *ingest.Ingest
	query     *query.Engine
	compact   *compact.Compactor
	retention *retention.Retention
	shards    *shard.Manager
	metrics   *metric.Registry
	webDir    string
}

func main() {
	addr := flag.String("addr", ":8080", "http listen address")
	dataDir := flag.String("dir", "data", "data directory for WAL segments")
	window := flag.Duration("window", time.Hour, "shard time window")
	maxAge := flag.Duration("retention", 30*24*time.Hour, "retention window")
	downsample := flag.Duration("downsample", time.Minute, "downsample window")
	flushEvery := flag.Duration("flush", 10*time.Second, "periodic flush interval")
	compactEvery := flag.Duration("compact", time.Minute, "compact interval")
	webDir := flag.String("web", "web", "directory of static web assets")
	flag.Parse()

	walDir := filepath.Join(*dataDir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		log.Fatalf("mkdir: %v", err)
	}

	reg := metric.New()
	idx := series.NewIndex(1024)
	w, err := wal.Open(walDir, 64<<20)
	if err != nil {
		log.Fatalf("wal open: %v", err)
	}
	defer w.Close()
	shards, err := shard.NewManager(*window, time.Now)
	if err != nil {
		log.Fatalf("shards: %v", err)
	}
	store := storage.NewStore()
	in, err := ingest.New(ingest.Options{
		Index:          idx,
		WAL:            w,
		Shards:         shards,
		Store:          store,
		Metrics:        reg,
		FlushThreshold: 4096,
		Concurrency:    16,
	})
	if err != nil {
		log.Fatalf("ingest: %v", err)
	}
	if err := in.Recover(); err != nil {
		log.Fatalf("recover: %v", err)
	}

	q := query.New(in.Mem(), shards, store, idx)
	cp := compact.New(store, shards, reg, model.DownsamplePolicy{Window: *downsample})
	rt := retention.New(store, shards, reg, model.RetentionPolicy{MaxAge: *maxAge, SweepStep: time.Minute})

	srv := &server{ingest: in, query: q, compact: cp, retention: rt, shards: shards, metrics: reg, webDir: *webDir}
	reg.Gauge("shards").Set(int64(shards.Count()))

	mux := http.NewServeMux()
	mux.HandleFunc("/probe", srv.handleProbe)
	mux.HandleFunc("/api/write", srv.handleWrite)
	mux.HandleFunc("/api/query", srv.handleQuery)
	mux.HandleFunc("/api/stats", srv.handleStats)
	mux.HandleFunc("/api/admin/expand", srv.handleExpand)
	mux.HandleFunc("/api/admin/compact", srv.handleAdminCompact)
	mux.HandleFunc("/api/admin/sweep", srv.handleAdminSweep)
	mux.HandleFunc("/api/admin/rotate", srv.handleAdminRotate)
	mux.HandleFunc("/metrics", srv.handleMetrics)
	mux.HandleFunc("/console", srv.handleConsole)
	mux.HandleFunc("/", srv.handleRoot)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		ticker := time.NewTicker(*flushEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := in.Flush(); err != nil {
					log.Printf("flush: %v", err)
				}
				if err := w.Sync(); err != nil {
					log.Printf("wal sync: %v", err)
				}
				reg.Gauge("shards").Set(int64(shards.Count()))
				reg.Gauge("blocks").Set(int64(store.Count()))
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(*compactEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := cp.CompactAll(); err != nil {
					log.Printf("compact: %v", err)
				}
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(rt.Policy().SweepStep)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				res := rt.Sweep(time.Now().UnixNano())
				reg.Gauge("blocks_removed").Add(int64(res.Removed))
			}
		}
	}()

	go srv.selfCheck()

	log.Printf("metricstore listening on %s (dir=%s)", *addr, *dataDir)
	httpServer := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}

func (s *server) selfCheck() {
	time.Sleep(200 * time.Millisecond)
	now := time.Now().UnixNano()
	if err := s.ingest.Append("selfcheck", map[string]string{"check": "1"}, now, 42.0); err != nil {
		log.Printf("selfcheck write failed: %v", err)
		return
	}
	sid, err := s.query.SeriesID("selfcheck", map[string]string{"check": "1"})
	if err != nil {
		log.Printf("selfcheck series: %v", err)
		return
	}
	res, err := s.query.QueryRange(sid, model.QueryRange{Start: now - 1, End: now + 1}, query.AggAvg, time.Minute.Nanoseconds())
	if err != nil {
		log.Printf("selfcheck query failed: %v", err)
		return
	}
	log.Printf("selfcheck: %d samples, %d shards, %d blocks", len(res), s.shards.Count(), s.ingest.Store().Count())
}

func (s *server) handleProbe(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","shards":%d,"blocks":%d,"series":%d}`,
		s.shards.Count(), s.ingest.Store().Count(), s.ingest.Index().Len())
}

func (s *server) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("Content-Type") == "application/json" {
		var batch []ingest.BulkPoint
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		if err := s.ingest.AppendBatch(batch); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, `{"ok":true,"bulk":%d}`, len(batch))
		return
	}
	name := r.URL.Query().Get("name")
	tsRaw := r.URL.Query().Get("ts")
	valRaw := r.URL.Query().Get("value")
	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		http.Error(w, "bad ts", http.StatusBadRequest)
		return
	}
	val, err := strconv.ParseFloat(valRaw, 64)
	if err != nil {
		http.Error(w, "bad value", http.StatusBadRequest)
		return
	}
	labels := parseLabels(r.URL.Query().Get("labels"))
	if err := s.ingest.Append(name, labels, ts, val); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, `{"ok":true}`)
}

func (s *server) handleQuery(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	start, err1 := strconv.ParseInt(r.URL.Query().Get("start"), 10, 64)
	end, err2 := strconv.ParseInt(r.URL.Query().Get("end"), 10, 64)
	if err1 != nil || err2 != nil {
		http.Error(w, "bad range", http.StatusBadRequest)
		return
	}
	agg := query.AggAvg
	switch r.URL.Query().Get("agg") {
	case "sum":
		agg = query.AggSum
	case "min":
		agg = query.AggMin
	case "max":
		agg = query.AggMax
	case "count":
		agg = query.AggCount
	case "delta":
		agg = query.AggDelta
	}
	step, _ := strconv.ParseInt(r.URL.Query().Get("step"), 10, 64)
	if step <= 0 {
		step = time.Minute.Nanoseconds()
	}
	labels := parseLabels(r.URL.Query().Get("labels"))
	sid, err := s.query.SeriesID(name, labels)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if r.URL.Query().Get("multi") == "1" {
		res, err := s.query.QueryMulti([]uint64{sid}, model.QueryRange{Start: start, End: end}, agg, step)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)
		return
	}
	res, err := s.query.QueryRange(sid, model.QueryRange{Start: start, End: end}, agg, step)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func (s *server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"shards": s.shards.Stats(),
		"blocks": s.ingest.Store().Stats(),
		"series": s.ingest.Index().Len(),
		"buffer": s.ingest.Mem().Stats(),
		"wal":    s.ingest.WAL().Info(),
	})
}

func (s *server) handleExpand(w http.ResponseWriter, r *http.Request) {
	n, err := strconv.Atoi(r.URL.Query().Get("buckets"))
	if err != nil || n <= 0 {
		http.Error(w, "bad buckets", http.StatusBadRequest)
		return
	}
	if err := s.ingest.Index().Expand(n); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, `{"buckets":%d}`, s.ingest.Index().BucketCount())
}

func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprint(w, s.metrics.Snapshot())
}

func (s *server) handleConsole(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, filepath.Join(s.webDir, "console.html"))
}

func (s *server) handleRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/console", http.StatusFound)
}

func parseLabels(raw string) map[string]string {
	labels := make(map[string]string)
	if raw == "" {
		return labels
	}
	for _, pair := range strings.Split(raw, ",") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 && kv[0] != "" {
			labels[kv[0]] = kv[1]
		}
	}
	return labels
}
