package managedconsumer

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/roncewind/go-util/queues/sqs"
	"github.com/roncewind/go-util/util"
	"github.com/senzing/g2-sdk-go/g2api"
	"github.com/senzing/go-common/record"
	"github.com/sourcegraph/conc/pool"
)

var jobPool chan SQSJob

type ManagedConsumerError struct {
	error
}

// ----------------------------------------------------------------------------
// Job implementation
// ----------------------------------------------------------------------------

// define a structure that will implement the Job interface
type SQSJob struct {
	client    *sqs.Client
	engine    g2api.G2engine
	id        int
	message   types.Message
	usedCount int
	withInfo  bool
}

// ----------------------------------------------------------------------------

// Job interface implementation:
// Execute() is run once for each Job
func (j *SQSJob) Execute(ctx context.Context) error {
	fmt.Println("DEBUG: start job execute. msg id:", *j.message.MessageId)
	// increment the number of times this job struct was used and return to the pool
	defer func() {
		j.usedCount++
		fmt.Println("DEBUG: end job execute. msg id:", *j.message.MessageId, "count:", j.usedCount)
		jobPool <- *j
	}()
	// fmt.Printf("Received a message- msgId: %s, msgCnt: %d, ConsumerTag: %s\n", id, *j.message.MessageCount, *j.message.ConsumerTag)
	record, newRecordErr := record.NewRecord(string(*j.message.Body))
	if newRecordErr == nil {
		fmt.Println("DEBUG: msg id:", *j.message.MessageId, "record id:", record.Id)
		loadID := "Load"
		if j.withInfo {
			var flags int64 = 0
			_, withInfoErr := j.engine.AddRecordWithInfo(ctx, record.DataSource, record.Id, record.Json, loadID, flags)
			if withInfoErr != nil {
				fmt.Printf("Record in error: %s:%s:%s:%s\n", *j.message.MessageId, loadID, record.DataSource, record.Id)
				return withInfoErr
			}
			//TODO:  what do we do with the "withInfo" data here?
			// fmt.Printf("Record added: %s:%s:%s:%s\n", *j.message.MessageId, loadID, record.DataSource, record.Id)
			// fmt.Printf("WithInfo: %s\n", withInfo)
		} else {
			addRecordErr := j.engine.AddRecord(ctx, record.DataSource, record.Id, record.Json, loadID)
			// fmt.Println("Record added:", record.Id, "MessageId:", *j.message.MessageId)
			if addRecordErr != nil {
				fmt.Printf("ERROR: Add Record error: %s:%s:%s:%s\n", *j.message.MessageId, loadID, record.DataSource, record.Id)
				return addRecordErr
			}
		}

		// when we successfully process a message, delete it.
		//as long as there was no error delete the message from the queue
		err := j.client.RemoveMessage(ctx, j.message)
		if err != nil {
			fmt.Println("ERROR: Record not removed from queue. msg id:", *j.message.MessageId, "record:", record, "error:", err)
		}
		fmt.Println("DEBUG: Record removed from queue. msg id:", *j.message.MessageId)
	} else {
		// logger.LogMessageFromError(MessageIdFormat, 2001, "create new szRecord", newRecordErr)
		fmt.Println(time.Now(), "ERROR: Invalid delivery from SQS. msg id:", *j.message.MessageId)
		// when we get an invalid delivery, send to the dead letter queue
		err := j.client.PushDeadRecord(ctx, j.message)
		if err != nil {
			fmt.Println("ERROR: Unable to push message to the dead letter queue. msg id:", *j.message.MessageId, "record:", record, "error:", err)
		}
	}
	return nil
}

// ----------------------------------------------------------------------------

// Whenever Execute() returns an error or panics, this is called
func (j *SQSJob) OnError(err error) {
	// TODO: look at the error codes and only requeue when they are retryable
	// for now, just requeue if they haven't been requeued before
	// TODO:  on error should the record get put back into the record channel?
	fmt.Println("ERROR: Worker error:", err)
	fmt.Println("ERROR: Failed to add record. msg id:", *j.message.MessageId)
	err = j.client.PushDeadRecord(context.Background(), j.message)
	if err != nil {
		fmt.Println("ERROR: Pushing message to the dead letter queue. msg id:", *j.message.MessageId, "error:", err)
	}
}

// ----------------------------------------------------------------------------
// -- add records in SQS to Senzing
// ----------------------------------------------------------------------------

// Starts a number of workers that read Records from the given queue and add
// them to Senzing.
// - Workers restart when they are killed or die.
// - respond to standard system signals.
func StartManagedConsumer(ctx context.Context, urlString string, numberOfWorkers int, g2engine g2api.G2engine, withInfo bool) error {

	if g2engine == nil {
		return errors.New("G2 Engine not set, unable to start the managed consumer")
	}

	//default to the max number of OS threads
	if numberOfWorkers <= 0 {
		numberOfWorkers = runtime.GOMAXPROCS(0)
	}
	fmt.Println(time.Now(), "Number of consumer workers:", numberOfWorkers)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	client, err := sqs.NewClient(ctx, urlString)
	if err != nil {
		return ManagedConsumerError{util.WrapError(err, "unable to get a new SQS client")}
	}
	defer client.Close()

	// setup jobs that will be used to process SQS deliveries
	jobPool = make(chan SQSJob, numberOfWorkers)
	for i := 0; i < numberOfWorkers; i++ {
		jobPool <- SQSJob{
			client:    client,
			engine:    g2engine,
			id:        i,
			usedCount: 0,
			withInfo:  withInfo,
		}
	}

	messages, err := client.Consume(ctx)
	if err != nil {
		fmt.Println(time.Now(), "Error getting delivery channel:", err)
		return ManagedConsumerError{util.WrapError(err, "unable to get a new SQS message channel")}
	}

	p := pool.New().WithMaxGoroutines(numberOfWorkers)
	jobCount := 0
	for message := range messages {
		job := <-jobPool
		job.message = message
		fmt.Println("DEBUG: add to job queue. jobCount:", jobCount, "msg id:", *job.message.MessageId)
		p.Go(func() {
			err := job.Execute(ctx)
			if err != nil {
				job.OnError(err)
			}
		})

		jobCount++
		if jobCount%10000 == 0 {
			fmt.Println(time.Now(), "INFO: Jobs added to job queue:", jobCount)
		}
	}

	// Wait for all the records in the record channel to be processed
	p.Wait()

	// clean up after ourselves
	close(jobPool)
	// drain the job pool
	var job SQSJob
	ok := true
	for ok {
		job, ok = <-jobPool
		fmt.Println("Job:", job.id, "used:", job.usedCount)
	}

	return nil
}
