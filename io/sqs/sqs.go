package sqs

import (
	"context"

	"github.com/roncewind/go-util/queues"
	"github.com/roncewind/go-util/queues/sqs"
	"github.com/senzing/g2-sdk-go/g2api"
)

// ----------------------------------------------------------------------------

// Start moving records in the recordchan to SQS
func StartProducer(ctx context.Context, urlString string, numberOfWorkers int, recordchan <-chan queues.Record) {
	client := sqs.NewClient(ctx, urlString)
}

// ----------------------------------------------------------------------------

// Start processing records in SQS
func StartConsumer(ctx context.Context, urlString string, numberOfWorkers int, g2engine *g2api.G2engine, withInfo bool) error {
	return nil
}
