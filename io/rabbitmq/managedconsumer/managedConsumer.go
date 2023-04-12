package managedconsumer

import (
	"context"
	"fmt"
	"runtime"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/roncewind/go-util/queues/rabbitmq"
	"github.com/roncewind/go-util/util"
	"github.com/senzing/g2-sdk-go/g2api"
	"github.com/senzing/go-common/record"
	"github.com/sourcegraph/conc/pool"
)

var jobPool chan *RabbitJob

type ManagedConsumerError struct {
	error
}

// ----------------------------------------------------------------------------
// Job implementation
// ----------------------------------------------------------------------------

// define a structure that will implement the Job interface
type RabbitJob struct {
	delivery  amqp.Delivery
	engine    *g2api.G2engine
	id        int
	usedCount int
	withInfo  bool
}

// ----------------------------------------------------------------------------

// Job interface implementation:
// Execute() is run once for each Job
func (j *RabbitJob) Execute(ctx context.Context) error {
	// increment the number of times this job struct was used and return to the pool
	defer func() {
		j.usedCount++
		jobPool <- j
	}()
	// fmt.Printf("Received a message- msgId: %s, msgCnt: %d, ConsumerTag: %s\n", id, j.delivery.MessageCount, j.delivery.ConsumerTag)
	record, newRecordErr := record.NewRecord(string(j.delivery.Body))
	if newRecordErr == nil {
		loadID := "Load"
		if j.withInfo {
			var flags int64 = 0
			_, withInfoErr := (*j.engine).AddRecordWithInfo(ctx, record.DataSource, record.Id, record.Json, loadID, flags)
			if withInfoErr != nil {
				// fmt.Println(time.Now(), "Error adding record:", j.delivery.MessageId, "error:", withInfoErr)
				fmt.Printf("Record in error: %s:%s:%s:%s\n", j.delivery.MessageId, loadID, record.DataSource, record.Id)
				return withInfoErr
			}
			//TODO:  what do we do with the "withInfo" data here?
			// fmt.Printf("Record added: %s:%s:%s:%s\n", j.delivery.MessageId, loadID, record.DataSource, record.Id)
			// fmt.Printf("WithInfo: %s\n", withInfo)
		} else {
			addRecordErr := (*j.engine).AddRecord(ctx, record.DataSource, record.Id, record.Json, loadID)
			if addRecordErr != nil {
				// fmt.Println(time.Now(), "Error adding record:", j.delivery.MessageId, "error:", addRecordErr)
				fmt.Printf("Record in error: %s:%s:%s:%s\n", j.delivery.MessageId, loadID, record.DataSource, record.Id)
				return addRecordErr
			}
		}

		// when we successfully process a delivery, acknowledge it.
		j.delivery.Ack(false)
	} else {
		// logger.LogMessageFromError(MessageIdFormat, 2001, "create new szRecord", newRecordErr)
		fmt.Println(time.Now(), "Invalid delivery from RabbitMQ:", j.delivery.MessageId)
		// when we get an invalid delivery, negatively acknowledge and send to the dead letter queue
		j.delivery.Nack(false, false)
	}
	return nil
}

// ----------------------------------------------------------------------------

// Whenever Execute() returns an error or panics, this is called
func (j *RabbitJob) OnError(err error) {
	// TODO: look at the error codes and only requeue when they are retryable
	// for now, just requeue if they haven't been requeued before
	// TODO:  on error should the record get put back into the record channel?
	fmt.Println("Worker error:", err)
	fmt.Println("Failed to move record:", j.id)
	if j.delivery.Redelivered {
		j.delivery.Nack(false, false)
	} else {
		j.delivery.Nack(false, true)
	}
}

// ----------------------------------------------------------------------------
// -- add records in RabbitMQ to Senzing
// ----------------------------------------------------------------------------

// Starts a number of workers that read Records from the given queue and add
// them to Senzing.
// - Workers restart when they are killed or die.
// - respond to standard system signals.
func StartManagedConsumer(ctx context.Context, urlString string, numberOfWorkers int, g2engine *g2api.G2engine, withInfo bool) error {

	//default to the max number of OS threads
	if numberOfWorkers <= 0 {
		numberOfWorkers = runtime.GOMAXPROCS(0)
	}
	fmt.Println(time.Now(), "Number of consumer workers:", numberOfWorkers)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// setup jobs that will be used to process RabbitMQ deliveries
	jobPool = make(chan *RabbitJob, numberOfWorkers)
	for i := 0; i < numberOfWorkers; i++ {
		jobPool <- &RabbitJob{
			engine:    g2engine,
			id:        i,
			usedCount: 0,
			withInfo:  withInfo,
		}
	}

	client, err := rabbitmq.NewClient(urlString)
	if err != nil {
		return ManagedConsumerError{util.WrapError(err, "unable to get a new RabbitMQ client")}
	}
	defer client.Close()

	deliveries, err := client.Consume(numberOfWorkers)
	if err != nil {
		fmt.Println(time.Now(), "Error getting delivery channel:", err)
		return ManagedConsumerError{util.WrapError(err, "unable to get a new RabbitMQ delivery channel")}
	}

	p := pool.New().WithMaxGoroutines(numberOfWorkers)
	jobCount := 0
	for delivery := range deliveries {
		job := <-jobPool
		job.delivery = delivery
		p.Go(func() {
			err := job.Execute(ctx)
			if err != nil {
				job.OnError(err)
			}
		})

		jobCount++
		if jobCount%10000 == 0 {
			fmt.Println(time.Now(), "Jobs added to job queue:", jobCount)
		}
	}

	// Wait for all the records in the record channel to be processed
	p.Wait()

	// clean up after ourselves
	close(jobPool)
	// drain the job pool
	var job *RabbitJob
	ok := true
	for ok {
		job, ok = <-jobPool
		fmt.Println("Job:", job.id, "used:", job.usedCount)
	}

	return nil
}
