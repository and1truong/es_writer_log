package main

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/google/uuid"
	. "github.com/andytruong/es_writer_log"

	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/olivere/elastic.v5/config"
)

func main() {
	ctx := context.Background()
	queueUrl := Env("RABBITMQ_URL", "")
	con, _ := Connection(queueUrl)
	ch := Channel(con, "topic", "events")
	routingKey := Env("RABBITMQ_ROUTING_KEY", "")
	bulkSize, _ := strconv.Atoi(Env("ELASTIC_SEARCH_BULK_SIZE", "500"))
	stream := Stream(ch, "events", "es-writer-log", []string{routingKey}, bulkSize)
	esUrl := Env("ELASTIC_SEARCH_URL", "http://localhost:9200/?sniff=false")
	esIndex := Env("ELASTIC_SEARCH_LOG_INDEX", "es-writer-log")
	docType := Env("ELASTIC_SEARCH_LOG_DOC_TYPE", "bulk-request")

	cfg, _ := config.Parse(esUrl)
	es, err := elastic.NewClientFromConfig(cfg)
	if err != nil {
		panic("failed to connect elastic search")
	}

	bulk := es.Bulk()

	for m := range stream {
		payload := map[string]interface{}{}
		if err := json.Unmarshal(m.Body, &payload); err != nil {
			continue
		}

		bulk = bulk.Add(
			elastic.
				NewBulkIndexRequest().
				Index(esIndex).
				Type(docType).
				Id(uuid.New().String()).
				Doc(payload),
		)

		if bulkSize == bulk.NumberOfActions() {
			bulk.Do(ctx)
			ch.Ack(m.DeliveryTag, true)
		}
	}
}
