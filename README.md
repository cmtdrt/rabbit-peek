# rabbit-peek

Outil CLI en Go pour vérifier ponctuellement que des messages sont bien publiés sur un exchange RabbitMQ et reçus via une queue temporaire, sans infrastructure permanente ni modification de l'IaC.

## Utilisation

### Mode `--listen` (continu)

Surveille un flux en continu jusqu'à `Ctrl+C` ou une erreur de connexion :

```bash
rabbit-peek --listen \
  --exchange my.exchange \
  --routing-key orders.created \
  --host amqp://user:pass@localhost:5672/
```

### Mode `--once` (autonome)

Attend exactement N messages, ou s'arrête après un timeout :

```bash
rabbit-peek --once \
  --n-messages 5 \
  --timeout 30s \
  --exchange my.exchange \
  --routing-key orders.created \
  --host amqp://guest:guest@localhost:5672/
```

Utile en CI/CD pour valider qu'un message a bien transité :

```bash
rabbit-peek --once --n-messages 1 --timeout 10s \
  --exchange events --routing-key user.signup \
  --host amqp://user:pass@broker:5672/
```

Code de sortie `1` si le timeout est atteint avant d'avoir reçu N messages.

## Flags

| Flag | Description | Défaut |
|------|-------------|--------|
| `--listen` | Consommation continue jusqu'à SIGINT/SIGTERM | — |
| `--once` | Consommation de N messages puis arrêt | — |
| `--exchange` | Nom de l'exchange à surveiller (**requis**) | — |
| `--routing-key` | Routing key pour le bind (vide = bind sans clé) | `""` |
| `--host` | URL AMQP du broker (identifiants optionnels dans l'URL) | `amqp://guest:guest@localhost:5672/` |
| `--n-messages` | Nombre de messages attendus (mode `--once`) | `1` |
| `--timeout` | Timeout maximal (mode `--once`, ex: `30s`) | `30s` |
| `--log-file` | Fichier de log (console seule si vide) | — |
| `--format` | Format de sortie : `text` ou `json` | `text` |

## Format des logs

Chaque message consommé inclut :

- **timestamp**
- **routing_key**
- **exchange**
- **headers** (si présents)
- **body** (brut ; champ `body_json` en plus si le body est du JSON valide)

Exemple en `--format text` :

```
[2026-06-10T16:30:00.123456789Z] exchange=events routing_key=orders.created body={"id":42}
```

Exemple en `--format json` :

```json
{"timestamp":"2026-06-10T16:30:00.123456789Z","routing_key":"orders.created","exchange":"events","body":"{\"id\":42}","body_json":{"id":42}}
```

## Fonctionnement technique

1. Connexion au broker via [amqp091-go](https://github.com/rabbitmq/amqp091-go).
2. Création d'une queue temporaire server-named, exclusive et auto-delete.
3. Bind de cette queue sur l'exchange avec la routing key fournie.
4. Consommation des messages (ack manuel après log).
5. À l'arrêt, fermeture de la connexion → RabbitMQ supprime automatiquement la queue.

La connexion se fait via une URL AMQP passée à `--host`. Les identifiants (`user:pass`) sont optionnels et inclus dans l'URL si le broker les exige, par ex. `amqp://user:pass@host:5672/vhost`. Rien n'est stocké dans le code.

## Structure du projet

```
.
├── main.go          # Point d'entrée
├── cli/             # Parsing des flags CLI
├── rabbit/          # Connexion, queue temporaire, Peek (consommation)
├── logger/          # Formatage et écriture des logs
├── go.mod
└── README.md
```

## Licence

MIT
