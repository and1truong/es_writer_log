package es_writer_log

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func Env(name string, defaultValue string) string {
	if value := os.Getenv(name); "" != value {
		return value
	}

	return defaultValue
}

func Connection(queueUrl string) (*amqp.Connection, error) {
	con, err := amqp.Dial(queueUrl)

	if nil != err {
		logrus.
			WithError(err).
			Error("failed to make connection")

		return nil, err
	}

	go func() {
		conCloseChan := con.NotifyClose(make(chan *amqp.Error))

		select
		{
		case err := <-conCloseChan:
			logrus.
				WithError(err).
				Panic("connection broken")
		}
	}()

	return con, nil
}

func Channel(con *amqp.Connection, kind string, exchangeName string) *amqp.Channel {
	ch, err := con.Channel()
	if nil != err {
		logrus.WithError(err).Panic("failed to make channel")
	}

	if "topic" != kind && "direct" != kind {
		panic("unsupported channel kind: " + kind)
	}

	err = ch.ExchangeDeclare(exchangeName, kind, false, false, false, false, nil)
	if nil != err {
		panic(err.Error())
	}

	return ch
}

func Stream(ch *amqp.Channel, exchange string, queue string, routingKeys []string, prefetchCount int) <-chan amqp.Delivery {
	defer logrus.
		WithField("exchange", exchange).
		WithField("consumer", queue).
		WithField("routingKeys", routingKeys).
		Info("consumer started")

	_, err := ch.QueueDeclare(queue, false, false, false, false, nil)
	if nil != err {
		logrus.
			WithError(err).
			WithField("queue", queue).
			WithField("routingKeys", routingKeys).
			Panic("failed declaring queue")
	}

	for _, routingKey := range routingKeys {
		ch.QueueBind(queue, routingKey, exchange, true, nil)
	}

	err = ch.Qos(prefetchCount, 0, false)
	if nil != err {
		logrus.
			WithError(err).
			WithField("queue", queue).
			WithField("routingKeys", routingKeys).
			Panic("failed to setup qos")
	}

	messages, err := ch.Consume(queue, "", false, false, false, true, nil)
	if nil != err {
		panic(err.Error())
	}

	return messages
}
