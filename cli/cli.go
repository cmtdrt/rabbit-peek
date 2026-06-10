package cli

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"
)

// Config holds parsed CLI configuration.
type Config struct {
	Mode       string
	Exchange   string
	RoutingKey string
	Host       string
	NMessages  int
	Timeout    time.Duration
	LogFile    string
	Format     string
}

// Reads and validates command-line flags.
func Parse(args []string) (Config, error) {
	fs := flag.NewFlagSet("rabbit-peek", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	listen := fs.Bool("listen", false, "Consommer les messages en continu jusqu'à SIGINT/SIGTERM")
	once := fs.Bool("once", false, "Consommer exactement N messages puis s'arrêter")
	exchange := fs.String("exchange", "", "Nom de l'exchange à surveiller (requis)")
	routingKey := fs.String("routing-key", "", "Routing key pour le bind (optionnel)")
	host := fs.String("host", "amqp://guest:guest@localhost:5672/", "URL AMQP du broker (user:pass optionnels dans l'URL)")
	nMessages := fs.Int("n-messages", 1, "Nombre de messages à attendre (mode --once)")
	timeout := fs.Duration("timeout", 30*time.Second, "Timeout maximal en mode --once (ex: 30s)")
	logFile := fs.String("log-file", "", "Chemin du fichier de log (optionnel, console si vide)")
	format := fs.String("format", "text", "Format de log: text ou json")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	cfg := Config{
		Exchange:   *exchange,
		RoutingKey: *routingKey,
		Host:       *host,
		NMessages:  *nMessages,
		Timeout:    *timeout,
		LogFile:    *logFile,
		Format:     *format,
	}

	switch {
	case *listen && *once:
		return Config{}, errors.New("les modes --listen et --once sont mutuellement exclusifs")
	case *listen:
		cfg.Mode = "listen"
	case *once:
		cfg.Mode = "once"
	default:
		return Config{}, errors.New("un mode est requis: --listen ou --once")
	}

	if cfg.Exchange == "" {
		return Config{}, errors.New("--exchange est requis")
	}

	if cfg.Mode == "once" {
		if cfg.NMessages <= 0 {
			return Config{}, errors.New("--n-messages doit être > 0 en mode --once")
		}
		if cfg.Timeout <= 0 {
			return Config{}, errors.New("--timeout doit être > 0 en mode --once")
		}
	}

	switch cfg.Format {
	case "text", "json":
	default:
		return Config{}, fmt.Errorf("format inconnu %q: utiliser text ou json", cfg.Format)
	}

	return cfg, nil
}
