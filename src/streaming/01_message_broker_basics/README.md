# Task 01 — Use Basic Capabilities of Message Broker Systems (Apache Kafka)

Write a Kafka **producer** and **consumer**. The producer publishes the latest
ten r/programming posts and a comment per post as JSON to a Kafka topic; the
consumer subscribes to the same topic, retrieves all messages, and displays them.

This is a "program code" task, so the deliverable is the working producer +
consumer against a real Kafka broker.

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | Single-broker Apache Kafka in KRaft mode (no Zookeeper). The "broker message cluster". |
| `producer.py` | Connects to the broker, publishes 10 post messages + 10 comment messages to the topic. |
| `consumer.py` | Subscribes to the same topic, receives all messages, displays them (and all comments). |
| `README.md` | This file. |

## Message shapes (exactly as the task specifies)

- Post: `{"id": post.id, "title": post.title, "upvotes": post.score}`
- Comment: `{"id": post.id, "comment": <our comment>}`

Each message also carries a `type` field (`post` / `comment`) so the consumer can
tell them apart on the single topic.

## Reddit source (with offline fallback)

The producer fetches the latest 10 r/programming posts with **PRAW** when Reddit
API credentials are present in a gitignored `.env`:

```
REDDIT_CLIENT_ID=...
REDDIT_CLIENT_SECRET=...
REDDIT_USER_AGENT=pex-artifacts by u/yourname
```

If the credentials are absent (or the call fails), a built-in **sample
generator** produces ten posts in the exact same shape, so the program always
runs offline and the producer/consumer flow is fully demonstrated. The run log
states which path was used.

## Run

```bash
cd src/streaming/01_message_broker_basics
docker compose up -d          # start the Kafka broker (wait ~10s)

cd ../../..
uv run python src/streaming/01_message_broker_basics/producer.py
uv run python src/streaming/01_message_broker_basics/consumer.py

cd src/streaming/01_message_broker_basics
docker compose down -v        # tear down
```

## How to verify

- The producer logs: `published 20 messages (10 posts + 10 comments)`.
- The consumer prints every POST and COMMENT, then re-prints all comment messages
  in the required `{"id", "comment"}` shape, and logs
  `received 10 post messages and 10 comment messages`.
- Both programs end with an assertion and exit 0.

## Acceptance criteria mapping

| Criterion | Where |
|---|---|
| Producer connects to the Kafka broker cluster and sends to the topic | `producer.py` `connect_producer` + `producer.send` |
| Consumer subscribes to the same topic and retrieves messages | `consumer.py` `KafkaConsumer(TOPIC, ...)` |
| Producer sends messages with content | 10 posts + 10 comments in the required JSON |
| Consumer receives and displays on the console | POST/COMMENT log lines + comment summary |

## Dependencies

`kafka-python` (Kafka client) and `praw` (Reddit, optional) in the root
`pyproject.toml`; Docker (Colima) for the broker. No paid services.
