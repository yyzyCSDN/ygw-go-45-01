#!/usr/bin/env bash
set -euo pipefail
# Build-only image build for MetricStore. The platform contract builds this
# image; it intentionally does not run go test inside the image.
docker build -t metricstore:benzhi .
