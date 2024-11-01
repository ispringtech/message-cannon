package rabbit

import (
	"context"
	"errors"
	"slices"
	"time"

	"gopkg.in/tomb.v2"

	"github.com/leandro-lugaresi/hub"
	"github.com/streadway/amqp"

	"github.com/leandro-lugaresi/message-cannon/runner"
)

type consumer struct {
	runner        runner.Runnable
	hash          string
	name          string
	queue         string
	workerPool    pool
	timeout       time.Duration
	bufferTimeout time.Duration
	bufferSize    int
	factoryName   string
	opts          Options
	channel       *amqp.Channel
	t             tomb.Tomb
	hub           *hub.Hub
}

// Run start a goroutine to consume messages and pass to one runner.
func (c *consumer) Run() {
	c.t.Go(func() error {
		defer func() {
			err := c.channel.Close()
			if err != nil {
				c.hub.Publish(hub.Message{
					Name:   "rabbit.consumer.error",
					Body:   []byte("Error closing the consumer channel"),
					Fields: hub.Fields{"error": err},
				})
			}
		}()
		d, err := c.channel.Consume(c.queue, "rabbitmq-"+c.name+"-"+c.hash,
			c.opts.AutoAck,
			c.opts.Exclusive,
			c.opts.NoLocal,
			c.opts.NoWait,
			c.opts.Args)
		if err != nil {
			c.hub.Publish(hub.Message{
				Name:   "rabbit.consumer.error",
				Body:   []byte("Failed to start consume"),
				Fields: hub.Fields{"error": err},
			})
			return err
		}
		dying := c.t.Dying()
		closed := c.channel.NotifyClose(make(chan *amqp.Error))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		for {
			select {
			case <-dying:
				// When dying we wait for any remaining worker to finish
				c.workerPool.Wait()
				return nil
			case err := <-closed:
				return err
			case msg, ok := <-d:
				if !ok {
					c.hub.Publish(hub.Message{
						Name:   "rabbit.consumer.error",
						Body:   []byte("receive an empty delivery. closing consumer"),
						Fields: hub.Fields{},
					})
					return errors.New("receive an empty delivery")
				}

				msgs := []amqp.Delivery{msg}
				if c.bufferTimeout != 0 {
					msgs = c.consumeBuffer(d, msg)
				}

				// When maxWorkers goroutines are in flight, Acquire blocks until one of the
				// workers finishes.
				c.workerPool.Acquire()
				go func(msgs []amqp.Delivery) {
					nctx := ctx
					if c.timeout >= time.Second {
						var canc context.CancelFunc
						nctx, canc = context.WithTimeout(ctx, c.timeout)
						defer canc()
					}
					c.processMessages(nctx, msgs)
					c.workerPool.Release()
				}(slices.Clone(msgs))
			}
		}
	})
}

// Kill will try to stop the internal work.
func (c *consumer) Kill() {
	c.t.Kill(nil)
	<-c.t.Dead()
}

// Alive returns true if the tomb is not in a dying or dead state.
func (c *consumer) Alive() bool {
	return c.t.Alive()
}

// Name return the consumer name
func (c *consumer) Name() string {
	return c.name
}

// FactoryName is the name of the factory responsible for this consumer.
func (c *consumer) FactoryName() string {
	return c.factoryName
}

func (c *consumer) consumeBuffer(d <-chan amqp.Delivery, firstDelivery amqp.Delivery) []amqp.Delivery {
	deliveries := []amqp.Delivery{firstDelivery}

	for {
		select {
		case delivery, ok := <-d:
			if !ok {
				return deliveries
			}
			deliveries = append(deliveries, delivery)
			if len(deliveries) >= c.bufferSize {
				// dot not consume all messages on high message rate
				return deliveries
			}
			continue // go to next iteration
		case <-time.After(c.bufferTimeout):
			return deliveries
		}
	}
}

func (c *consumer) processMessages(ctx context.Context, msgs []amqp.Delivery) {
	var err error
	start := time.Now()

	messages := make([]runner.Message, 0, len(msgs))
	for _, msg := range msgs {
		messages = append(messages, runner.Message{
			Body:    msg.Body,
			Headers: getHeaders(msg),
		})
	}

	status, err := c.runner.Process(ctx, messages)
	duration := time.Since(start)
	fields := hub.Fields{
		"duration":    duration,
		"status-code": status,
	}
	topic := "rabbit.process.sucess"
	if err != nil {
		topic = "rabbit.process.error"
		switch e := err.(type) {
		case *runner.Error:
			fields["error"] = e.Err
			fields["exit-code"] = e.StatusCode
			fields["output"] = e.Output
		default:
			fields["error"] = e
		}
	}
	c.hub.Publish(hub.Message{
		Name:   topic,
		Fields: fields,
	})

	messagesAcknowdger := func(f func(amqp.Delivery) error) error {
		var firstErr error
		for _, msg := range msgs {
			err := f(msg)
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}

	switch status {
	case runner.ExitACK:
		err = messagesAcknowdger(func(msg amqp.Delivery) error {
			return msg.Ack(false)
		})
	case runner.ExitFailed:
		err = messagesAcknowdger(func(msg amqp.Delivery) error {
			return msg.Reject(true)
		})
	case runner.ExitRetry, runner.ExitNACKRequeue, runner.ExitTimeout:
		err = messagesAcknowdger(func(msg amqp.Delivery) error {
			return msg.Nack(false, true)
		})
	case runner.ExitNACK:
		err = messagesAcknowdger(func(msg amqp.Delivery) error {
			return msg.Nack(false, false)
		})
	default:
		c.hub.Publish(hub.Message{
			Name:   "rabbit.consumer.error",
			Body:   []byte("the runner returned an unexpected exitStatus. Message will be requeued."),
			Fields: hub.Fields{"status": status},
		})
		err = messagesAcknowdger(func(msg amqp.Delivery) error {
			return msg.Reject(true)
		})
	}
	if err != nil {
		c.hub.Publish(hub.Message{
			Name:   "rabbit.consumer.error",
			Body:   []byte("error during the acknowledgement phase"),
			Fields: hub.Fields{"error": err},
		})
	}
}
