package rabbitmq

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	exchangeName    string
	isReady         bool
	logger          *log.Logger
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	queueName       string
	reconnectDelay  time.Duration
	reInitDelay     time.Duration
	resendDelay     time.Duration
}

var (
	errNotConnected  = errors.New("not connected to a server")
	errAlreadyClosed = errors.New("already closed: not connected to the server")
	errShutdown      = errors.New("client is shutting down")
)

// ----------------------------------------------------------------------------
// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
func NewClient(exchangeName, queueName, addr string) *Client {
	client := Client{
		done:           make(chan bool),
		exchangeName:   exchangeName,
		logger:         log.New(os.Stdout, "", log.LstdFlags),
		queueName:      queueName,
		reconnectDelay: 5 * time.Second,
		reInitDelay:    2 * time.Second,
		resendDelay:    5 * time.Second,
	}
	go client.handleReconnect(addr)
	return &client
}

// ----------------------------------------------------------------------------
// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (client *Client) handleReconnect(addr string) {
	for {
		client.isReady = false
		client.logger.Println("Attempting to connect")

		conn, err := client.connect(addr)

		if err != nil {
			client.logger.Println("Failed to connect. Retrying...")

			select {
			case <-client.done:
				return
			case <-time.After(client.reconnectDelay):
			}
			continue
		}

		if done := client.handleReInit(conn); done {
			break
		}
	}
}

// ----------------------------------------------------------------------------
// connect will create a new AMQP connection
func (client *Client) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)

	if err != nil {
		return nil, err
	}

	client.changeConnection(conn)
	client.logger.Println("Connected!")
	return conn, nil
}

// ----------------------------------------------------------------------------
// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (client *Client) handleReInit(conn *amqp.Connection) bool {
	for {
		client.isReady = false

		err := client.init(conn)

		if err != nil {
			client.logger.Println("Failed to initialize channel. Retrying...")

			select {
			case <-client.done:
				return true
			case <-time.After(client.reInitDelay):
			}
			continue
		}

		select {
		case <-client.done:
			return true
		case <-client.notifyConnClose:
			client.logger.Println("Connection closed. Reconnecting...")
			return false
		case <-client.notifyChanClose:
			client.logger.Println("Channel closed. Re-running init...")
		}
	}
}

// ----------------------------------------------------------------------------
// init will initialize channel, declare the exchange, and declare the queue
func (client *Client) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	err = ch.Confirm(false)

	if err != nil {
		return err
	}
	err = ch.ExchangeDeclare(
		client.exchangeName, // name
		"direct",            // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		return err
	}

	var q amqp.Queue
	q, err = ch.QueueDeclare(
		client.queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		q.Name,              // queue name
		q.Name,              // routing key
		client.exchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	client.changeChannel(ch)
	client.isReady = true
	client.logger.Println("Setup!")

	return nil
}

// ----------------------------------------------------------------------------
// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (client *Client) changeConnection(connection *amqp.Connection) {
	client.connection = connection
	client.notifyConnClose = make(chan *amqp.Error, 1)
	client.connection.NotifyClose(client.notifyConnClose)
}

// ----------------------------------------------------------------------------
// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (client *Client) changeChannel(channel *amqp.Channel) {
	client.channel = channel
	client.notifyChanClose = make(chan *amqp.Error, 1)
	client.notifyConfirm = make(chan amqp.Confirmation, 1)
	client.channel.NotifyClose(client.notifyChanClose)
	client.channel.NotifyPublish(client.notifyConfirm)
}

// ----------------------------------------------------------------------------
// Push will push data onto the queue and wait for a confirm.
// If no confirm is received by the resendTimeout,
// it re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (client *Client) Push(body []byte, messageId string) error {
	if !client.isReady {
		return errors.New("failed to push: not connected") //TODO:  error message to include messageId?
	}
	for {
		err := client.UnsafePush(body, messageId)
		if err != nil {
			client.logger.Println("Push failed. Retrying. MessageId: ", messageId) //TODO:  debug or trace logging, add messageId
			select {
			case <-client.done:
				return errShutdown //TODO:  error message to include messageId?
			case <-time.After(client.resendDelay):
			}
			continue
		}
		select {
		case confirm := <-client.notifyConfirm:
			if confirm.Ack {
				client.logger.Println("Push confirmed!")
				return nil
			}
		case <-time.After(client.resendDelay):
		}
		client.logger.Println("Push didn't confirm. Retrying. MessageId: ", messageId) //TODO:  debug or trace logging, add messageId
	}
}

// ----------------------------------------------------------------------------
// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for if the server will
// receive the message.
func (client *Client) UnsafePush(body []byte, messageId string) error {
	if !client.isReady {
		return errNotConnected //TODO:  error message to include messageId?
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return client.channel.PublishWithContext(
		ctx,
		"",
		client.queueName,
		false,
		false,
		amqp.Publishing{
			Body:         body,
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			MessageId:    messageId,
		},
	)
}

// ----------------------------------------------------------------------------
// Consume will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (client *Client) Consume() (<-chan amqp.Delivery, error) {
	if !client.isReady {
		return nil, errNotConnected
	}

	if err := client.channel.Qos(
		1,
		0,
		false,
	); err != nil {
		return nil, err
	}

	return client.channel.Consume(
		client.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
}

// ----------------------------------------------------------------------------
// Close will cleanly shutdown the channel and connection.
func (client *Client) Close() error {
	if !client.isReady {
		return errAlreadyClosed
	}
	close(client.done)
	err := client.channel.Close()
	if err != nil {
		return err
	}
	err = client.connection.Close()
	if err != nil {
		return err
	}

	client.isReady = false
	return nil
}
