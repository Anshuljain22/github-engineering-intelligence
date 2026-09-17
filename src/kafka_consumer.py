import json
import os
from datetime import datetime
from kafka import KafkaConsumer

TOPICS = [
    "github_de.public.repositories",
    "github_de.public.commits",
    "github_de.public.issues",
    "github_de.public.pull_requests",
]

OUTPUT_DIR = "data/cdc"

os.makedirs(OUTPUT_DIR, exist_ok=True)

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=["localhost:29092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="github-de-python-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("Kafka CDC consumer started...")
print("Listening to:")
for topic in TOPICS:
    print(f"  - {topic}")

for message in consumer:
    event = message.value

    topic = message.topic
    table = topic.split(".")[-1]

    output_file = os.path.join(
        OUTPUT_DIR,
        f"{table}.jsonl"
    )

    record = {
        "received_at": datetime.now().isoformat(),
        "topic": topic,
        "partition": message.partition,
        "offset": message.offset,
        "event": event,
    }

    with open(output_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")

    payload = event.get("payload", {})
    operation = payload.get("op")

    print(
        f"[{table}] "
        f"operation={operation} "
        f"partition={message.partition} "
        f"offset={message.offset}"
    )