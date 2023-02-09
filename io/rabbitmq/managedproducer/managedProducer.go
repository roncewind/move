package managedproducer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/roncewind/move/io/rabbitmq"
	"github.com/roncewind/workerpool"
)

// ----------------------------------------------------------------------------
// Job implementation
// ----------------------------------------------------------------------------

// define a structure that will implement the Job interface
type RabbitJob struct {
	id      string
	jobQ    chan<- workerpool.Job
	timeout int
}

// ----------------------------------------------------------------------------

// make sure RabbitJob implements the Job interface
var _ workerpool.Job = (*RabbitJob)(nil)

// ----------------------------------------------------------------------------

// Job interface implementation
func (j *RabbitJob) Execute() error {
	fmt.Println(j.id, "executing")

	return nil
}

func (j *RabbitJob) OnError(err error) {
	fmt.Println(j.id, "error", err)
}

// ----------------------------------------------------------------------------

// Starts a number of workers that push Records in the record channel to
// the given queue.
// Workers restart when they are killed or die.
// Workers respond to standard system signals.
func StartManagedProducer(exchangeName, queueName, urlString string, numberOfWorkers int, recordchan chan rabbitmq.Record) chan struct{} {

	if numberOfWorkers <= 0 {
		numberOfWorkers = runtime.GOMAXPROCS(0)
	}
	fmt.Println("Number of producer workers:", numberOfWorkers)

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
			ctx:        ctx,
			id:         i,
			client:     rabbitmq.NewClient(exchangeName, queueName, urlString),
			recordchan: recordchan,
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
			}
		}
	}()

	//FIXME:  return blocking channel
	<-sigShutdown
}

// ----------------------------------------------------------------------------

// Starts a number of workers that push Records in the record channel to
// the given queue.
// Workers restart when they are killed or die.
// Workers respond to standard system signals.
func StartManagedProducerXXX(exchangeName, queueName, urlString string, numberOfWorkers int, recordchan chan rabbitmq.Record) {

	if numberOfWorkers <= 0 {
		numberOfWorkers = runtime.GOMAXPROCS(0)
	}
	fmt.Println("Number of producer workers:", numberOfWorkers)

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
			ctx:        ctx,
			id:         i,
			client:     rabbitmq.NewClient(exchangeName, queueName, urlString),
			recordchan: recordchan,
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
			}
		}
	}()

	//FIXME:  return blocking channel
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
	}()

	return wait
}

// ----------------------------------------------------------------------------
type Worker struct {
	ctx        context.Context
	err        error
	id         int
	shutdown   bool
	client     *rabbitmq.Client
	recordchan chan rabbitmq.Record
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
		case record, ok := <-worker.recordchan:
			if !ok && len(worker.recordchan) == 0 {
				// This means the channel is empty and closed.
				fmt.Println("All records moved, recordchan closed")
				worker.client.Close()
				worker.shutdown = true
				return nil
			}

			if err := worker.client.Push(record); err != nil {
				fmt.Println("Failed to publish record:", record.GetMessageId())
				fmt.Println("Error: ", err)
			}
		}
	}
}
