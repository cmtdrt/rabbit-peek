package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/cdrouet/rabbit-peek/cli"
	"github.com/cdrouet/rabbit-peek/logger"
	"github.com/cdrouet/rabbit-peek/rabbit"
)

func main() {
	cfg, err := cli.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "erreur: %v\n", err)
		os.Exit(2)
	}

	logWriter, err := logger.New(cfg.Format, cfg.LogFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "erreur: %v\n", err)
		os.Exit(1)
	}
	defer logWriter.Close()

	peek := rabbit.NewPeek(cfg.Host, cfg.Exchange, cfg.RoutingKey)
	if err := peek.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "erreur: %v\n", err)
		os.Exit(1)
	}
	defer peek.Close()

	fmt.Fprintf(os.Stderr, "bind sur exchange=%s routing_key=%q\n", cfg.Exchange, cfg.RoutingKey)

	os.Exit(run(cfg, peek, logWriter))
}

func run(cfg cli.Config, peek *rabbit.Peek, logWriter *logger.Writer) int {
	handler := func(d amqp.Delivery) error {
		return logWriter.Log(d)
	}

	switch cfg.Mode {
	case "listen":
		return runListen(peek, handler)
	case "once":
		return runOnce(cfg, peek, handler)
	default:
		fmt.Fprintf(os.Stderr, "mode inconnu: %s\n", cfg.Mode)
		return 2
	}
}

func runListen(peek *rabbit.Peek, handler rabbit.MessageHandler) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err := peek.Listen(ctx, handler)
	if err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "erreur: %v\n", err)
		return 1
	}
	return 0
}

func runOnce(cfg cli.Config, peek *rabbit.Peek, handler rabbit.MessageHandler) int {
	received, err := peek.Once(context.Background(), cfg.NMessages, cfg.Timeout, handler)
	if err != nil {
		fmt.Fprintf(os.Stderr, "erreur: %v\n", err)
		return 1
	}

	if received < cfg.NMessages {
		fmt.Fprintf(os.Stderr, "timeout: %d/%d messages reçus\n", received, cfg.NMessages)
		return 1
	}

	fmt.Fprintf(os.Stderr, "%d/%d messages reçus\n", received, cfg.NMessages)
	return 0
}
