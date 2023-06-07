package sqs

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/roncewind/go-util/queues"
	"github.com/roncewind/move/io/sqs/managedconsumer"
	"github.com/roncewind/move/io/sqs/managedproducer"
	"github.com/senzing/g2-sdk-go/g2api"
)

// ----------------------------------------------------------------------------

// Start moving records in the recordchan to SQS
func StartProducer(ctx context.Context, urlString string, numberOfWorkers int, recordchan <-chan queues.Record) {
	managedproducer.StartManagedProducer(ctx, urlString, numberOfWorkers, recordchan)
}

// ----------------------------------------------------------------------------

// Start processing records in SQS
func StartConsumer(ctx context.Context, urlString string, numberOfWorkers int, g2engine g2api.G2engine, withInfo bool, visibilitySeconds int32) (err error) {
	consumerContext, cancelFunc := context.WithCancel(ctx)
	go catchSignals(cancelFunc)
	return managedconsumer.StartManagedConsumer(consumerContext, urlString, numberOfWorkers, g2engine, withInfo, visibilitySeconds)
}

// ----------------------------------------------------------------------------

func catchSignals(cancel context.CancelFunc) {
	sig := make(chan os.Signal, 1)
	defer close(sig)

	// PONDER: add any other syscalls?
	// SIGHUP - hang up, lost controlling terminal
	// SIGINT - interrupt (ctrl-c)
	// SIGQUIT - quit (ctrl-\)
	// SIGTERM - request to terminate (this happens when scaling)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
	killsig := <-sig
	switch killsig {
	case syscall.SIGINT:
		fmt.Println("DEBUG SIG: Killed with ctrl-c")
	case syscall.SIGTERM:
		fmt.Println("DEBUG SIG: Killed with request to terminate")
	case syscall.SIGQUIT:
		fmt.Println("DEBUG SIG: Killed with ctrl-\\")
	case syscall.SIGHUP:
		fmt.Println("DEBUG SIG: Killed with hang up")
	}
	cancel()
}
