"""Kafka consumer for task 01 (basic message broker capabilities).

The consumer subscribes to the same topic the producer writes to, retrieves all
messages, and displays them on the console. It specifically collects and displays
the comment messages (as the task requires: "Receive all messages with your
comments and display all of them"), while also printing the post messages for
context.

Because a demo must terminate, the consumer reads from the earliest offset and
stops once it has been idle for a few seconds (no new messages), then prints a
summary.

Run (after the producer has published):
    uv run python src/streaming/01_message_broker_basics/consumer.py
"""

import json
import logging
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer")

BOOTSTRAP = "localhost:9092"
TOPIC = "reddit_programming"
GROUP_ID = "reddit_display_group"
IDLE_TIMEOUT_MS = 5000  # stop after 5s with no new messages


def connect_consumer(retries: int = 15, delay: float = 2.0) -> KafkaConsumer:
    """Connect to Kafka, retrying while the broker finishes starting up."""
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=GROUP_ID,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=IDLE_TIMEOUT_MS,
            )
        except KafkaError:
            log.info(f"Kafka not ready yet (attempt {attempt}/{retries}), waiting...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka broker at " + BOOTSTRAP)


def main() -> int:
    consumer = connect_consumer()
    log.info(f"Subscribed to topic '{TOPIC}', reading messages...")

    posts = []
    comments = []
    for message in consumer:  # ends after IDLE_TIMEOUT_MS of silence
        value = message.value
        if value.get("type") == "post":
            posts.append(value)
            log.info(f"POST    id={value['id']} upvotes={value['upvotes']} "
                     f"title={value['title']}")
        elif value.get("type") == "comment":
            comments.append(value)
            log.info(f"COMMENT id={value['id']} comment={value['comment']}")

    consumer.close()

    # The task asks specifically to display all messages with our comments.
    log.info("All received comment messages:")
    for c in comments:
        log.info(f"    {json.dumps({'id': c['id'], 'comment': c['comment']})}")

    log.info(f"VERIFICATION: received {len(posts)} post messages and "
             f"{len(comments)} comment messages from '{TOPIC}'.")
    assert comments, "no comment messages were received"
    log.info("Consumer finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
