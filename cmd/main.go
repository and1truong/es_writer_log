package main

import (
	"context"
	"strconv"

	. "github.com/andytruong/es_writer_log"

	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/olivere/elastic.v5/config"
)

func main() {
	ctx := context.Background()
	queueUrl := En("RABBITMQ_URL", "")
	con, _ := Connection(queueUrl)
	ch := Channel(con, "topic", "events")
	routingKey := Env("RABBITMQ_ROUTING_KEY", "")
	bulkSize, _ := strconv.Atoi(Env("BULK_SIZE", "500"))
	stream := Stream(ch, "events", "es-writer-log", []string{routingKey}, bulkSize)
	esIndex := Env("ES_LOG_INDEX", "es-writer-log")

	cfg, _ := config.Parse(Env("ELASTIC_SEARCH_URL", "http://localhost:9200/?sniff=false"))
	es, _ := elastic.NewClientFromConfig(cfg)
	bulk := es.Bulk()

	for m := range stream {
		r := elastic.
			NewBulkIndexRequest().
			Index(esIndex).
			Type("bulk-request").
			Doc(m.Body)

		bulk.Add(r)

		if bulkSize == bulk.NumberOfActions() {
			bulk.Do(ctx)
			ch.Ack(m.DeliveryTag, true)
		}
	}
}
