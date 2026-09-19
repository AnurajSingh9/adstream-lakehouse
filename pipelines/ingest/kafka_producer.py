"""Optional Kafka publisher for local demos.

Writes the same JSON contract as the batch generator onto topic `ad.events`.
Broker defaults to localhost:9092 — bring up docker compose first.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", type=Path, required=True)
    ap.add_argument("--topic", default="ad.events")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--sleep-ms", type=int, default=5)
    args = ap.parse_args()

    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError as e:
        raise SystemExit("pip install kafka-python if you want the live producer") from e

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    n = 0
    with args.file.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            producer.send(args.topic, json.loads(line))
            n += 1
            if args.sleep_ms:
                time.sleep(args.sleep_ms / 1000)
    producer.flush()
    print(f"published {n} → {args.topic}")


if __name__ == "__main__":
    main()
