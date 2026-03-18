FROM golang:1.26-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/cdc ./cmd/cdc

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /bin/cdc /usr/local/bin/cdc

EXPOSE 8080 9090

ENTRYPOINT ["cdc"]
CMD ["-config", "/etc/cdc/config.yaml"]
