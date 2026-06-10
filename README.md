# Rabbit-peek

Outil pour vérifier ponctuellement que des messages sont bien publiés sur un exchange RabbitMQ et reçus via une queue temporaire qui sera automatiquement supprimée, sans avoir à le faire à la main et sans modification de l'IaC.

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

(Code de sortie `1` si le timeout est atteint avant d'avoir reçu N messages)

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
