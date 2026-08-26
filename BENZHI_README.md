# MetricStore

MetricStore 是一个时序指标存储引擎，提供指标点写入、时间分片、WAL 持久化、
内存表批量落盘、下采样与保留策略，以及按时间范围的跨分片聚合查询。

## 构建与运行

依赖已 vendor 到 `vendor/`，可离线构建：

```bash
go build -mod=vendor ./...
go vet -mod=vendor ./...
go test -mod=vendor ./...
```

启动服务：

```bash
go run -mod=vendor ./cmd/metricstore -addr :8080 -dir data \
  -window 1h -retention 720h -downsample 1m
```

## HTTP 接口

- `GET /probe` 健康探测
- `POST /api/write?name=cpu.usage&labels=host=node-1&ts=<ns>&value=0.5` 写入一个指标点
- `GET /api/query?name=cpu.usage&labels=host=node-1&start=<ns>&end=<ns>&agg=avg&step=60000` 范围聚合查询
- `GET /api/stats` 引擎统计
- `GET /metrics` 运行指标
- `GET /console` 浏览器控制台页面

## 容器镜像

`benzhi.Dockerfile` 以构建产物为准（build-only），使用离线 vendor 构建，镜像内
不执行测试。
