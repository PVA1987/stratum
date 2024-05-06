# Many thanks to original author Brandon Smith (onemorebsmith).
FROM golang:1.19.1 as builder

LABEL org.opencontainers.image.description="Dockerized spectre Stratum Bridge"
LABEL org.opencontainers.image.authors="spectre Community"
LABEL org.opencontainers.image.source="https://github.com/GRinvestPOOL/spectre-stratum-bridge"

WORKDIR /go/src/app
ADD go.mod .
ADD go.sum .
RUN go mod download

ADD . .
RUN go build -o /go/bin/app ./cmd/spectrebridge


FROM gcr.io/distroless/base:nonroot
COPY --from=builder /go/bin/app /
COPY cmd/spectrebridge/config.yaml /

WORKDIR /
ENTRYPOINT ["/app"]
