#!/usr/bin/env python3
"""
Hackathon: Lime Fleet Ops - Data Publisher

Publishes pre-generated JSON ride events to Confluent Cloud Kafka with Avro
serialization and Schema Registry integration.

Timestamps are already aligned to current time by the generator script.

Usage:
    uv run publish_hackathon_data
    uv run publish_hackathon_data --data-file assets/hackathon/data/ride_events.jsonl
    uv run publish_hackathon_data --dry-run
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

try:
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import (
        SerializationContext,
        MessageField,
        StringSerializer,
    )
    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False

from .common.cloud_detection import auto_detect_cloud_provider, validate_cloud_provider, suggest_cloud_provider
from .common.terraform import extract_kafka_credentials, validate_terraform_state, get_project_root
from .common.logging_utils import setup_logging


# Avro schema for ride_events value
VALUE_SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "ride_events_value",
    "namespace": "org.apache.flink.avro.generated.record",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "rider_email", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "zone", "type": "string"},
        {"name": "origin_zone", "type": "string"},
        {"name": "destination_zone", "type": "string"},
        {"name": "vehicle_id", "type": "string"},
        {"name": "battery_level", "type": "int"},
        {"name": "ride_duration_minutes", "type": "double"},
        {"name": "ride_cost", "type": "double"},
        {"name": "event_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
})


class HackathonDataPublisher:
    """Publisher for hackathon ride events using AvroSerializer."""

    def __init__(
        self,
        bootstrap_servers: str,
        kafka_api_key: str,
        kafka_api_secret: str,
        schema_registry_url: str,
        schema_registry_api_key: str,
        schema_registry_api_secret: str,
        dry_run: bool = False,
    ):
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)

        sr_conf = {
            "url": schema_registry_url,
            "basic.auth.user.info": f"{schema_registry_api_key}:{schema_registry_api_secret}",
        }
        sr_client = SchemaRegistryClient(sr_conf)

        self.value_serializer = AvroSerializer(sr_client, VALUE_SCHEMA_STR)
        self.key_serializer = StringSerializer("utf_8")

        self.producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "sasl.mechanisms": "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.username": kafka_api_key,
            "sasl.password": kafka_api_secret,
            "linger.ms": 10,
            "batch.size": 16384,
            "compression.type": "snappy",
            "topic.metadata.refresh.interval.ms": "60000",
            "message.timeout.ms": "30000",
        }

        self.producer = None
        if not dry_run:
            self.producer = Producer(self.producer_config)

    def purge_topic(self, topic: str) -> None:
        """Delete all existing records from the topic before publishing."""
        from confluent_kafka.admin import AdminClient, OffsetSpec
        from confluent_kafka import TopicPartition as AdminTopicPartition

        self.logger.info(f"Purging existing records from topic '{topic}'...")
        admin = AdminClient({
            "bootstrap.servers": self.producer_config["bootstrap.servers"],
            "sasl.mechanisms": self.producer_config["sasl.mechanisms"],
            "security.protocol": self.producer_config["security.protocol"],
            "sasl.username": self.producer_config["sasl.username"],
            "sasl.password": self.producer_config["sasl.password"],
        })

        try:
            metadata = admin.list_topics(topic=topic, timeout=10)
            if topic not in metadata.topics:
                self.logger.info(f"Topic '{topic}' not found - skipping purge")
                return
            partition_ids = list(metadata.topics[topic].partitions.keys())
            tps = [AdminTopicPartition(topic, p) for p in partition_ids]

            futures = admin.list_offsets({tp: OffsetSpec.latest() for tp in tps})
            delete_offsets = {}
            for tp, future in futures.items():
                result = future.result()
                if result.offset > 0:
                    delete_offsets[tp] = AdminTopicPartition(tp.topic, tp.partition, result.offset)

            if delete_offsets:
                del_futures = admin.delete_records(delete_offsets)
                for tp, future in del_futures.items():
                    future.result()
                self.logger.info(f"Purged {len(delete_offsets)} partition(s) in '{topic}'")
            else:
                self.logger.info(f"Topic '{topic}' already empty")
        except Exception as e:
            self.logger.warning(f"Could not purge topic '{topic}': {e} - continuing without purge")

    def publish_jsonl_file(self, jsonl_file: Path, topic: str) -> Dict[str, int]:
        """Publish all JSON events from JSONL file with Avro serialization."""
        results = {"success": 0, "failed": 0, "total": 0}

        try:
            with open(jsonl_file, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f if line.strip()]
        except Exception as e:
            self.logger.error(f"Failed to read JSONL file {jsonl_file}: {e}")
            return results

        results["total"] = len(lines)
        self.logger.info(f"Found {len(lines)} events to publish")

        if not self.dry_run:
            self.purge_topic(topic)

        # Pre-fetch topic metadata to avoid mid-publish refresh issues
        if not self.dry_run:
            self.logger.info("Pre-fetching topic metadata...")
            self.producer.poll(0)
            import time
            time.sleep(2)

        for idx, line in enumerate(lines, 1):
            try:
                event = json.loads(line)

                if self.dry_run:
                    self.logger.debug(f"[DRY RUN] Would publish: {event['event_id']}")
                    results["success"] += 1
                    continue

                ctx_value = SerializationContext(topic, MessageField.VALUE)
                serialized_value = self.value_serializer(event, ctx_value)

                ctx_key = SerializationContext(topic, MessageField.KEY)
                serialized_key = self.key_serializer(event["rider_email"], ctx_key)

                self.producer.produce(
                    topic,
                    key=serialized_key,
                    value=serialized_value,
                )
                results["success"] += 1

                # Poll frequently to handle delivery reports and keep metadata fresh
                self.producer.poll(0)

                if idx % 1000 == 0:
                    self.producer.flush()
                    self.logger.info(
                        f"Progress: {idx}/{results['total']} events "
                        f"({results['success']} succeeded, {results['failed']} failed)"
                    )

            except Exception as e:
                self.logger.error(f"Failed to publish event {idx}: {e}")
                results["failed"] += 1

        if not self.dry_run and self.producer:
            self.logger.info("Flushing remaining messages...")
            self.producer.flush()

        return results

    def close(self):
        if self.producer:
            self.producer.flush()


def main():
    parser = argparse.ArgumentParser(
        description="Publish Lime ride events to Kafka for hackathon"
    )
    parser.add_argument(
        "--data-file",
        type=Path,
        default=Path("assets/hackathon/data/ride_events.jsonl"),
        help="Path to JSONL data file",
    )
    parser.add_argument(
        "--topic",
        default="ride_events",
        help="Kafka topic name (default: ride_events)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Test without publishing")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()
    logger = setup_logging(args.verbose)

    if not args.data_file.exists():
        logger.error(f"Data file does not exist: {args.data_file}")
        return 1

    if not CONFLUENT_KAFKA_AVAILABLE:
        logger.error("confluent-kafka not available. Install with: uv pip install confluent-kafka[avro,schema-registry]")
        return 1

    cloud_provider = auto_detect_cloud_provider()
    if not cloud_provider:
        suggestion = suggest_cloud_provider()
        if suggestion:
            cloud_provider = suggestion
        else:
            logger.error("Could not auto-detect cloud provider.")
            return 1

    try:
        project_root = get_project_root()
        validate_terraform_state(cloud_provider, project_root)
        credentials = extract_kafka_credentials(cloud_provider, project_root)
    except Exception as e:
        logger.error(f"Failed to get credentials: {e}")
        return 1

    try:
        publisher = HackathonDataPublisher(
            bootstrap_servers=credentials["bootstrap_servers"],
            kafka_api_key=credentials["kafka_api_key"],
            kafka_api_secret=credentials["kafka_api_secret"],
            schema_registry_url=credentials["schema_registry_url"],
            schema_registry_api_key=credentials["schema_registry_api_key"],
            schema_registry_api_secret=credentials["schema_registry_api_secret"],
            dry_run=args.dry_run,
        )
    except Exception as e:
        logger.error(f"Failed to initialize publisher: {e}")
        return 1

    try:
        logger.info(f"Publishing ride events to topic '{args.topic}'")
        if args.dry_run:
            logger.info("[DRY RUN MODE]")

        results = publisher.publish_jsonl_file(args.data_file, args.topic)

        print(f"\n{'=' * 60}")
        print("HACKATHON DATA PUBLISHING SUMMARY")
        print(f"{'=' * 60}")
        print(f"Total records:  {results['total']}")
        print(f"Published:      {results['success']}")
        print(f"Failed:         {results['failed']}")
        print(f"{'=' * 60}")

        if args.dry_run:
            print("\n[DRY RUN COMPLETE - No messages were actually published]")

        return 0 if results["failed"] == 0 else 1
    finally:
        publisher.close()


if __name__ == "__main__":
    sys.exit(main())
