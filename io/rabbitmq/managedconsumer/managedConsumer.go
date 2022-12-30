package managedconsumer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/roncewind/move/io/rabbitmq"
	"github.com/roncewind/szrecord"
	"github.com/senzing/g2-sdk-go/g2engine"
)

// ----------------------------------------------------------------------------
type Worker struct {
	client   *rabbitmq.Client
	ctx      context.Context
	err      error
	g2engine g2engine.G2engine
	id       int
	shutdown bool
	withInfo bool
}

// ----------------------------------------------------------------------------
// Starts a number of workers that consume Records from the given queue.
// Workers restart when they are killed or die.
// Workers respond to standard system signals.
func StartManagedConsumer(exchangeName, queueName, urlString string, numberOfWorkers int, g2engine g2engine.G2engine, withInfo bool) {

	if numberOfWorkers <= 0 {
		numberOfWorkers = runtime.GOMAXPROCS(0)
	}
	fmt.Println("Number of consumer workers:", numberOfWorkers)

	// make a buffered channel with the space for all workers
	//  workers will signal on this channel if they die
	workerChan := make(chan *Worker, numberOfWorkers)
	// PONDER:  close the workerChan here or in the goroutine?
	//  probably doesn't matter in this case, but something to keep an eye on.
	defer close(workerChan)

	ctx, cancel := context.WithCancel(context.Background())
	// when shutdown signalled by OS signal, wait for 15 seconds for graceful shutdown
	//	 to complete, then force
	sigShutdown := gracefulShutdown(cancel, 15*time.Second)

	// start up a number of workers.
	for i := 0; i < numberOfWorkers; i++ {
		i := i
		worker := &Worker{
			client:   rabbitmq.NewClient(exchangeName, queueName, urlString),
			ctx:      ctx,
			g2engine: g2engine,
			id:       i,
			withInfo: withInfo,
		}
		go worker.Start(workerChan)
	}

	// Monitor a chan and start a new worker if one has stopped:
	//   - read the channel
	//	 - block until something is written
	//   - check if worker is shutting down
	//	 	- if not, re-start the worker
	go func() {
		shutdownCount := numberOfWorkers
		for worker := range workerChan {

			if worker.shutdown {
				shutdownCount--
			} else {
				// log the error
				fmt.Printf("Worker %d stopped with err: %s\n", worker.id, worker.err)
				// reset err
				worker.err = nil

				// a goroutine has ended, restart it
				go worker.Start(workerChan)
				fmt.Printf("Worker %d restarted\n", worker.id)
			}

			if shutdownCount == 0 {
				fmt.Println("All workers shutdown, exiting")
				sigShutdown <- struct{}{}
				// os.Exit(0) //FIXME: rework shutdown so it doesn't exit. (just return?)
			}
		}
	}()

	<-sigShutdown
}

// ----------------------------------------------------------------------------
// gracefulShutdown waits for terminating syscalls then signals workers to shutdown
func gracefulShutdown(cancel func(), timeout time.Duration) chan struct{} {
	wait := make(chan struct{})

	go func() {
		defer close(wait)
		sig := make(chan os.Signal, 1)
		defer close(sig)

		// PONDER: add any other syscalls?
		// SIGHUP - hang up, lost controlling terminal
		// SIGINT - interrupt (ctrl-c)
		// SIGQUIT - quit (ctrl-\)
		// SIGTERM - request to terminate
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
		killsig := <-sig
		switch killsig {
		case syscall.SIGINT:
			fmt.Println("Killed with ctrl-c")
		case syscall.SIGTERM:
			fmt.Println("Killed with request to terminate")
		case syscall.SIGQUIT:
			fmt.Println("Killed with ctrl-\\")
		case syscall.SIGHUP:
			fmt.Println("Killed with hang up")
		}

		// set timeout for the cleanup to be done to prevent system hang
		timeoutSignal := make(chan struct{})
		timeoutFunc := time.AfterFunc(timeout, func() {
			fmt.Printf("Timeout %.1fs have elapsed, force exit\n", timeout.Seconds())
			close(timeoutSignal)
		})

		defer timeoutFunc.Stop()

		// cancel the context
		cancel()
		fmt.Println("Shutdown signalled.")

		// wait for timeout to finish
		<-timeoutSignal
		wait <- struct{}{}
		// os.Exit(0) //FIXME: shouldn't exit here, should just push into wait?
	}()

	return wait
}

// ----------------------------------------------------------------------------
// this function can start a new worker and re-start a worker if it's failed
func (worker *Worker) Start(workerChan chan<- *Worker) (err error) {
	// make the goroutine signal its death, whether it's a panic or a return
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				worker.err = err
			} else {
				worker.err = fmt.Errorf("panic happened %v", r)
			}
		} else {
			worker.err = err
		}
		workerChan <- worker
	}()
	worker.shutdown = false
	return worker.doWork()
}

// ----------------------------------------------------------------------------
// this function simulates do work as a worker
// PONDER:  private function, should only be called from Start?
func (worker *Worker) doWork() (err error) {
	// Worker simulation
	fmt.Println(worker.id, " doing work")
	deliveries, err := worker.client.Consume()
	for {
		// use select to test if our context has completed
		select {
		case <-worker.ctx.Done():
			now := time.Now()
			fmt.Printf("Worker %d cancelled\n", worker.id)
			err := worker.client.Close()
			worker.shutdown = true
			if err != nil {
				fmt.Println("Error closing RabbitMQ client.")
			}
			fmt.Printf("Worker %d shutdown with cancel, after %.1f.\n", worker.id, time.Since(now).Seconds())
			return nil
		case delivery := <-deliveries:
			fmt.Printf("Received a message- msgId: %s, msgCnt: %d, ConsumerTag: %s\n", delivery.MessageId, delivery.MessageCount, delivery.ConsumerTag)
			record, newRecordErr := szrecord.NewRecord(string(delivery.Body))
			if newRecordErr == nil {
				fmt.Printf("Processing record: %s\n", record.Id)
				loadID := "Load"
				if worker.withInfo {
					var flags int64 = 0
					withInfo, withInfoErr := worker.g2engine.AddRecordWithInfo(worker.ctx, record.DataSource, record.Id, record.Json, loadID, flags)
					if withInfoErr != nil {
						fmt.Println(withInfoErr.Error())
						// logger.LogMessage(MessageIdFormat, 2002, withInfoErr.Error())
					} else {
						//TODO:  what do we do with the record here?
						fmt.Printf("Record added: %s:%s:%s:%s\n", delivery.MessageId, loadID, record.DataSource, record.Id)
						fmt.Printf("WithInfo: %s\n", withInfo)
					}
				} else {
					addRecordErr := worker.g2engine.AddRecord(worker.ctx, record.DataSource, record.Id, record.Json, loadID)
					if addRecordErr != nil {
						fmt.Println(addRecordErr.Error())
						// logger.LogMessage(MessageIdFormat, 2003, addRecordErr.Error())
					} else {
						fmt.Printf("Record added: %s:%s:%s:%s\n", delivery.MessageId, loadID, record.DataSource, record.Id)
					}
				}

				// when we successfully process a delivery, Ack it.
				delivery.Ack(false)
				// when there's an issue with a delivery should we requeue it?
				// d.Nack(false, true)
			} else {
				// logger.LogMessageFromError(MessageIdFormat, 2001, "create new szRecord", newRecordErr)
				// when we get an invalid delivery, Ack it, so we don't requeue
				// TODO: set up rabbit with a dead letter queue?
				delivery.Ack(false)
				// FIXME: errors should be specific to the input method
				//  ala rabbitmq message ID?
			}
		}
	}
}
