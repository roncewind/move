package sqs

import (
	"context"
	"fmt"

	"github.com/roncewind/go-util/queues"
	"github.com/roncewind/go-util/queues/sqs"
	"github.com/roncewind/go-util/util"
	"github.com/senzing/g2-sdk-go/g2api"
)

// ----------------------------------------------------------------------------

// Start moving records in the recordchan to SQS
func StartProducer(ctx context.Context, urlString string, numberOfWorkers int, recordchan <-chan queues.Record) {

	fmt.Println("Get new sqs client")
	client, err := sqs.NewClient(ctx, urlString)
	if err != nil {
		fmt.Println("SQS new client error:", err)
		return
	}
	fmt.Println("SQS client:", client)
	for record := range recordchan {
		err := client.Push(ctx, record)
		if err != nil {
			fmt.Println("Error pushing record:", err)
			fmt.Println("Record:", record)
		}
	}

}

// ----------------------------------------------------------------------------

// Start processing records in SQS
func StartConsumer(ctx context.Context, urlString string, numberOfWorkers int, g2engine *g2api.G2engine, withInfo bool) (err error) {

	fmt.Println("Get new sqs client")
	client, err := sqs.NewClient(ctx, urlString)
	if err != nil {
		fmt.Println("SQS new client error:", err)
		return
	}
	fmt.Println("SQS client:", client)
	msgChan, err := client.Consume(ctx)
	for record := range util.OrDone(ctx, msgChan) {
		fmt.Println(record.Body)
	}
	return nil
}
