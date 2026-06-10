package rabbit

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MessageHandler is called for each consumed message.
type MessageHandler func(amqp.Delivery) error

// Peek connects to RabbitMQ, binds a temporary queue and consumes messages.
type Peek struct {
	url        string
	exchange   string
	routingKey string
	conn       *amqp.Connection
	channel    *amqp.Channel
	queueName  string
}

// NewPeek creates a Peek without connecting yet.
func NewPeek(url, exchange, routingKey string) *Peek {
	return &Peek{
		url:        url,
		exchange:   exchange,
		routingKey: routingKey,
	}
}

// Connect establishes the AMQP connection and declares/binds the temporary queue.
func (p *Peek) Connect() error {
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return fmt.Errorf("connexion RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("ouverture du channel: %w", err)
	}

	q, err := ch.QueueDeclare(
		"",    // server-named queue
		false, // not durable
		true,  // auto-delete when connection closes
		true,  // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("déclaration de la queue: %w", err)
	}

	if err := ch.QueueBind(q.Name, p.routingKey, p.exchange, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("bind queue=%s exchange=%s routing_key=%q: %w", q.Name, p.exchange, p.routingKey, err)
	}

	p.conn = conn
	p.channel = ch
	p.queueName = q.Name
	return nil
}

// Close closes the channel and connection; RabbitMQ removes the temporary queue.
func (p *Peek) Close() {
	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
}

// Listen consumes messages continuously until ctx is cancelled or an error occurs.
func (p *Peek) Listen(ctx context.Context, handler MessageHandler) error {
	deliveries, err := p.channel.Consume(
		p.queueName,
		"rabbit-peek",
		false, // manual ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("démarrage de la consommation: %w", err)
	}

	connErr := p.conn.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-connErr:
			if err != nil {
				return fmt.Errorf("connexion fermée: %w", err)
			}
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("channel de consommation fermé")
			}
			if err := handler(delivery); err != nil {
				_ = delivery.Nack(false, true)
				return err
			}
			if err := delivery.Ack(false); err != nil {
				return fmt.Errorf("ack message: %w", err)
			}
		}
	}
}

// Once consumes up to n messages or until timeout, whichever comes first.
func (p *Peek) Once(ctx context.Context, n int, timeout time.Duration, handler MessageHandler) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	deliveries, err := p.channel.Consume(
		p.queueName,
		"rabbit-peek",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return 0, fmt.Errorf("démarrage de la consommation: %w", err)
	}

	connErr := p.conn.NotifyClose(make(chan *amqp.Error, 1))
	received := 0

	for received < n {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return received, nil
			}
			return received, ctx.Err()
		case err := <-connErr:
			if err != nil {
				return received, fmt.Errorf("connexion fermée: %w", err)
			}
			return received, nil
		case delivery, ok := <-deliveries:
			if !ok {
				return received, fmt.Errorf("channel de consommation fermé")
			}
			if err := handler(delivery); err != nil {
				_ = delivery.Nack(false, true)
				return received, err
			}
			if err := delivery.Ack(false); err != nil {
				return received, fmt.Errorf("ack message: %w", err)
			}
			received++
		}
	}

	return received, nil
}
