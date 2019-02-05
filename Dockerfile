FROM golang:1.11

WORKDIR   /es_writer_log/
COPY    . /es_writer_log/

RUN cd /es_writer_log/
RUN go mod vendor
RUN CGO_ENABLED=0 GOOS=linux go build -o /app cmd/main.go

FROM alpine:3.8
RUN apk add --no-cache ca-certificates
COPY --from=0 /app /app

ENTRYPOINT ["/app"]
