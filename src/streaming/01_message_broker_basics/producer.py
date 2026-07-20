"""Kafka producer for task 01 (basic message broker capabilities).

The producer publishes, to a single Kafka topic, two kinds of messages for the
latest ten posts from the r/programming subreddit:

  1. post message:    {"id": post.id, "title": post.title, "upvotes": post.score}
  2. comment message: {"id": post.id, "comment": <our comment>}

Reddit source:
  If Reddit API credentials are present (in a gitignored .env: REDDIT_CLIENT_ID,
  REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT) the posts are fetched live with PRAW.
  If they are absent, a built-in sample generator produces ten posts in the exact
  same shape, so the program always runs offline and the producer/consumer flow
  is fully demonstrated either way.

Run (after 'docker compose up -d'):
    uv run python src/streaming/01_message_broker_basics/producer.py
"""

import json
import logging
import os
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("producer")

BOOTSTRAP = "localhost:9092"
TOPIC = "reddit_programming"
NUM_POSTS = 10


def fetch_posts_from_reddit() -> list[dict] | None:
    """Fetch the latest 10 r/programming posts via PRAW, or None if unavailable."""
    from dotenv import load_dotenv

    load_dotenv()
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT")
    if not (client_id and client_secret and user_agent):
        return None

    try:
        import praw

        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )
        posts = []
        for post in reddit.subreddit("programming").new(limit=NUM_POSTS):
            posts.append({"id": post.id, "title": post.title, "score": post.score})
        log.info(f"Fetched {len(posts)} posts from Reddit via PRAW.")
        return posts
    except Exception as exc:
        log.warning(f"Reddit fetch failed ({exc}); using the sample generator.")
        return None


def sample_posts() -> list[dict]:
    """Generate 10 sample posts in the same shape as the Reddit API returns."""
    titles = [
        "Why Rust is gaining traction in systems programming",
        "Understanding Python's GIL in 2026",
        "A deep dive into Kafka's KRaft mode",
        "Structured concurrency explained",
        "The case for monorepos",
        "How Spark Structured Streaming handles state",
        "Zero-downtime database migrations",
        "Writing a toy compiler in a weekend",
        "SQL window functions you should know",
        "Debugging distributed systems with tracing",
    ]
    return [
        {"id": f"post{i:02d}", "title": title, "score": 100 + i * 7}
        for i, title in enumerate(titles, start=1)
    ]


def make_comment(post: dict) -> str:
    """Produce our own short comment for a given post."""
    return f"Great read on '{post['title'][:40]}' - thanks for sharing."


def connect_producer(retries: int = 15, delay: float = 2.0) -> KafkaProducer:
    """Connect to Kafka, retrying while the broker finishes starting up."""
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
        except KafkaError:
            log.info(f"Kafka not ready yet (attempt {attempt}/{retries}), waiting...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka broker at " + BOOTSTRAP)


def main() -> int:
    posts = fetch_posts_from_reddit() or sample_posts()
    posts = posts[:NUM_POSTS]

    producer = connect_producer()
    log.info(f"Connected to Kafka at {BOOTSTRAP}, publishing to topic '{TOPIC}'.")

    sent = 0
    for post in posts:
        # 1. the post message
        post_msg = {"id": post["id"], "title": post["title"], "upvotes": post["score"]}
        producer.send(TOPIC, key=post["id"], value={"type": "post", **post_msg})
        sent += 1

        # 2. our comment message for that post
        comment_msg = {"id": post["id"], "comment": make_comment(post)}
        producer.send(TOPIC, key=post["id"], value={"type": "comment", **comment_msg})
        sent += 1

    producer.flush()
    producer.close()

    log.info(f"VERIFICATION: published {sent} messages "
             f"({len(posts)} posts + {len(posts)} comments) to '{TOPIC}'.")
    assert sent == 2 * len(posts), "expected one post and one comment per item"
    log.info("Producer finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
