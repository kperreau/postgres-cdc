FROM golang:1.24-alpine AS builder

RUN apk add --no-cache ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/cdc ./cmd/cdc

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /bin/cdc /usr/local/bin/cdc

RUN addgroup -S cdc && adduser -S cdc -G cdc
USER cdc

EXPOSE 8080 9090

ENTRYPOINT ["cdc"]
CMD ["-config", "/etc/cdc/config.yaml"]
