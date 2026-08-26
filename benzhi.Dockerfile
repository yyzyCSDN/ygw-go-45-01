FROM golang:1.23

WORKDIR /app

COPY . .

ENV GOPROXY=off \
    GOSUMDB=off \
    CGO_ENABLED=0

RUN go build -mod=vendor -o /out/metricstore ./cmd/metricstore

EXPOSE 8080

CMD ["/out/metricstore", "-addr", ":8080", "-dir", "/data", "-web", "/app/web"]
