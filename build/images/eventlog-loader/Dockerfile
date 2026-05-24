ARG GO_BASE_IMAGE=golang:1.26-bookworm
FROM ${GO_BASE_IMAGE} AS build

WORKDIR /src
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux /usr/local/go/bin/go build -mod=vendor -trimpath -ldflags="-s -w" -o /out/spark-eventlog-loader .

FROM scratch
COPY --from=build /out/spark-eventlog-loader /spark-eventlog-loader
ENTRYPOINT ["/spark-eventlog-loader"]
