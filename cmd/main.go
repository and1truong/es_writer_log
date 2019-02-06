package main

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/olivere/elastic.v5/config"
	"strconv"
	. "github.com/andytruong/es_writer_log"
)

type Payload struct {
	Body string `json:"body"`
	Uri  string `json:"uri"`
}

func main() {
	ctx := context.Background()
	queueUrl := Env("RABBITMQ_URL", "")
	con, _ := Connection(queueUrl)
	ch := Channel(con, "topic", "events")
	routingKey := Env("RABBITMQ_ROUTING_KEY", "")
	bulkSize, _ := strconv.Atoi(Env("ELASTIC_SEARCH_BULK_SIZE", "500"))
	stream := Stream(ch, "events", "es-writer-log", []string{routingKey}, bulkSize)
	esIndex := Env("ELASTIC_SEARCH_LOG_INDEX", "es-writer-log")
	docType := Env("ELASTIC_SEARCH_LOG_DOC_TYPE", "bulk-request")

	cfg, _ := config.Parse(Env("ELASTIC_SEARCH_URL", "http://localhost:9200/?sniff=false"))
	es, err := elastic.NewClientFromConfig(cfg)
	if err != nil {
		panic("failed to connect elastic search")
	}

	bulk := es.Bulk()

	for m := range stream {
		payload := Payload{}

		if err := json.Unmarshal(m.Body, &payload); err != nil {
			panic(err)
		}

		uuid := uuid.New()
		r := elastic.
			NewBulkIndexRequest().
			Index(esIndex).
			Type(docType).
			Id(uuid.String()).
			Doc(payload)

		bulk = bulk.Add(r)
		if bulkSize == bulk.NumberOfActions() {
			bulk.Do(ctx)
			ch.Ack(m.DeliveryTag, true)
		}
	}
}
