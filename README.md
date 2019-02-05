#es-writer logger
===

Tracker requests #es-writer handled

    env RABBITMQ_URL=amqp://user:pass@127.0.0.1:5672/ \
        RABBITMQ_ROUTING_KEY=${ES_WRITER_ROUTING_KEY} \
        ELASTIC_SEARCH_URL
        ELASTIC_SEARCH_LOG_INDEX=es-writer-log \
        ELASTIC_SEARCH_BULK_SIZE=500 \
        go run cmd/main.go
