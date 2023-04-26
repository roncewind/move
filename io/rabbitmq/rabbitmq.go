package rabbitmq

import (
	"context"

	"github.com/roncewind/go-util/queues"
	"github.com/roncewind/move/io/rabbitmq/managedconsumer"
	"github.com/roncewind/move/io/rabbitmq/managedproducer"
	"github.com/senzing/g2-sdk-go/g2api"
)

// ----------------------------------------------------------------------------

// Start moving records in the recordchan to RabbitMQ
func StartProducer(ctx context.Context, urlString string, numberOfWorkers int, recordchan <-chan queues.Record) {
	managedproducer.StartManagedProducer(ctx, urlString, numberOfWorkers, recordchan)
}

// ----------------------------------------------------------------------------

// Start processing records in RabbitMQ
func StartConsumer(ctx context.Context, urlString string, numberOfWorkers int, g2engine *g2api.G2engine, withInfo bool) error {
	return managedconsumer.StartManagedConsumer(ctx, urlString, numberOfWorkers, g2engine, withInfo)
}
