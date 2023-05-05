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
	client.PushBatch(ctx, recordchan)
	if err != nil {
		fmt.Println("Error pushing record batch:", err)
	}
	// for record := range recordchan {
	// 	err := client.Push(ctx, record)
	// 	if err != nil {
	// 		fmt.Println("Error pushing record:", err)
	// 		fmt.Println("Record:", record)
	// 	}
	// }

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
		fmt.Println("Record Body:", *record.Body) //TODO: added Senzing here
		//TODO: watch how long processing is taking and update the visibility timeout
		//TODO: on error add message to Dead Letter Queue

		err := client.PushDeadRecord(ctx, record)

		//as long as there was no error delete the message from the queue
		err = client.RemoveMessage(ctx, record)
		if err != nil {
			fmt.Println("Error removing message", record)
		}
	}
	return nil
}
