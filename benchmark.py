#!/usr/bin/env python3
"""Reproducible single-broker Kafka pilot and Pub/Sub Lite capacity model."""

from __future__ import annotations

import argparse
import csv
import math
import os
import resource
import shutil
import statistics
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CSV_FIELDS = [
    "benchmark_version",
    "timestamp_utc",
    "run_kind",
    "origin",
    "system",
    "run_id",
    "topic",
    "repetition",
    "status",
    "events_requested",
    "payload_bytes",
    "partitions",
    "replication_factor",
    "acks",
    "compression",
    "linger_ms",
    "batch_size_bytes",
    "idempotence",
    "max_in_flight",
    "retention_ms",
    "retention_bytes",
    "cleanup_policy",
    "segment_bytes",
    "segment_ms",
    "topic_creation_ms",
    "producer_init_ms",
    "metadata_ms",
    "warmup_topic",
    "warmup_total_ms",
    "warmup_ack_ms",
    "consumer_init_ms",
    "consumer_assignment_ms",
    "consumer_first_message_ms",
    "publish_duration_s",
    "consumer_duration_s",
    "consumer_post_assignment_s",
    "confirmed_events",
    "unconfirmed_events",
    "primary_recovery_percent",
    "retention_elapsed_from_publish_s",
    "retention_elapsed_from_initial_consume_s",
    "long_retention_delay_s",
    "long_retention_elapsed_from_publish_s",
    "long_retention_elapsed_from_initial_consume_s",
    "long_retention_unique_recovered",
    "long_retention_missing_events",
    "peak_rss_mib",
    "errors_detail",
    "effective_config",
    "historical_record_note",
    "publish_messages_per_s",
    "publish_mib_per_s",
    "consume_messages_per_s",
    "consume_mib_per_s",
    "ack_latency_p50_ms",
    "ack_latency_p95_ms",
    "ack_latency_p99_ms",
    "unique_recovered",
    "missing_events",
    "duplicate_events",
    "error_count",
    "retention_delay_s",
    "retention_unique_recovered",
    "retention_missing_events",
    "model_publish_capacity_mib_per_s",
    "model_subscribe_capacity_mib_per_s",
    "model_event_storage_bytes",
    "model_retention_capacity_events",
    "model_retention_capacity_gib",
    "model_max_publish_events_per_s",
    "model_max_subscribe_events_per_s",
    "model_payload_publish_mib_per_s",
    "model_payload_subscribe_mib_per_s",
    "throughput_mean_messages_per_s",
    "throughput_sample_sd_messages_per_s",
    "throughput_cv_percent",
    "recovery_percent",
    "loss_percent",
    "retention_5s_recovery_percent",
    "retention_30s_recovery_percent",
    "model_assumptions",
    "model_sources",
]


def percentile(values: list[float], pct: float) -> float | str:
    if not values:
        return ""
    ordered = sorted(values)
    position = (len(ordered) - 1) * pct / 100
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] * (upper - position) + ordered[upper] * (position - lower)


def append_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    if not write_header:
        with path.open(newline="", encoding="utf-8") as existing:
            reader = csv.reader(existing)
            current_header = next(reader, [])
        if current_header != CSV_FIELDS:
            raise ValueError(
                f"{path} uses an older CSV schema; migrate while preserving its rows first"
            )
    with path.open("a", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def statistical_summary(path: Path) -> list[dict[str, Any]]:
    """Append scenario summaries from exactly three valid definitive runs."""
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(newline="", encoding="utf-8") as stream:
        all_rows = list(csv.DictReader(stream))
    summaries = []
    for event_count in (10_000, 100_000, 1_000_000):
        selected = [
            row
            for row in all_rows
            if row.get("benchmark_version") == "0.3.1-audit"
            and row.get("run_kind") == "definitive"
            and row.get("status") == "ok"
            and int(row.get("events_requested") or 0) == event_count
        ]
        if len(selected) != 3 or {int(r["repetition"]) for r in selected} != {1, 2, 3}:
            raise ValueError(
                f"scenario {event_count} requires exactly three valid definitive repetitions; "
                f"found {len(selected)}"
            )
        if any(
            row.get("run_kind") == "statistical_summary"
            and row.get("benchmark_version") == "0.3.1-audit"
            and int(row.get("events_requested") or 0) == event_count
            for row in all_rows
        ):
            raise ValueError(
                f"summary already recorded for {event_count}; refusing to overwrite/duplicate"
            )

        throughput = [float(row["publish_messages_per_s"]) for row in selected]
        confirmed_total = sum(int(row["confirmed_events"]) for row in selected)
        recovered = sum(int(row["unique_recovered"]) for row in selected)
        missing = sum(int(row["missing_events"]) for row in selected)
        retention5 = sum(int(row["retention_unique_recovered"]) for row in selected)
        retention30 = sum(int(row["long_retention_unique_recovered"]) for row in selected)
        mean = statistics.mean(throughput)
        sd = statistics.stdev(throughput)  # sample SD, n-1
        summaries.append(
            {
                "benchmark_version": "0.3.1-audit",
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "run_kind": "statistical_summary",
                "origin": "resumen_estadistico_mediciones_reales",
                "system": "Apache Kafka",
                "run_id": f"SUMMARY-{event_count}-n3",
                "repetition": "n=3",
                "status": "summary_valid_definitive_runs_only",
                "events_requested": event_count,
                "throughput_mean_messages_per_s": mean,
                "throughput_sample_sd_messages_per_s": sd,
                "throughput_cv_percent": sd / mean * 100 if mean else "",
                "recovery_percent": recovered / confirmed_total * 100,
                "loss_percent": missing / confirmed_total * 100,
                "retention_5s_recovery_percent": retention5 / confirmed_total * 100,
                "retention_30s_recovery_percent": retention30 / confirmed_total * 100,
                "model_assumptions": (
                    "derived summary only; n=3 valid definitive repetitions; "
                    "sample SD uses n-1; CV=sample SD/mean; latency percentiles "
                    "remain per-run and are not averaged or pooled"
                ),
            }
        )
    append_rows(path, summaries)
    return summaries


def analytical_model(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Calculate documented capacity/storage bounds; never reports measured rates."""
    if not 1 <= args.partitions:
        raise ValueError("partitions debe ser >= 1")
    if not 4 <= args.model_publish_mib_per_s <= 16:
        raise ValueError("Pub/Sub Lite zonal publish por partición documentado: 4–16 MiB/s")
    if not 4 <= args.model_subscribe_mib_per_s <= 32:
        raise ValueError("Pub/Sub Lite zonal subscribe por partición documentado: 4–32 MiB/s")
    if args.storage_gib < 30:
        raise ValueError("El almacenamiento mínimo documentado por partición es 30 GiB")

    # A 1024-byte body with no key or attributes incurs the documented 12-byte
    # event timestamp in stored-size accounting. Capacity is not observed service
    # performance and does not imply a latency, delivery, or loss value.
    stored_event_bytes = args.size_bytes + 12
    storage_bytes = args.storage_gib * (1 << 30) * args.partitions
    pub_capacity = min(
        args.partitions * args.model_publish_mib_per_s,
        64.0,  # Default zonal publish quota without a reservation, per zone.
    )
    sub_capacity = min(
        args.partitions * args.model_subscribe_mib_per_s,
        128.0,  # Default zonal subscribe quota without a reservation, per zone.
    )
    pub_max = pub_capacity * (1 << 20) / stored_event_bytes
    sub_max = sub_capacity * (1 << 20) / stored_event_bytes
    payload_pub_mib = pub_max * args.size_bytes / (1 << 20)
    payload_sub_mib = sub_max * args.size_bytes / (1 << 20)
    storage_event_capacity = storage_bytes // stored_event_bytes
    assumptions = (
        "zonal/no reservation; stated per-partition provisioned capacity; "
        "one zone; 1024-byte data body, no attributes/order key, +12-byte "
        "event timestamp for storage accounting; uniform partition use; "
        "4 MiB/s capacity is applied to service-accounted 1036-byte messages; "
        "payload throughput is separately calculated as events/s * 1024 bytes; "
        "capacity bound, not measured throughput; latency/loss not modeled"
    )
    sources = (
        "https://cloud.google.com/pubsub/lite/docs/topics;"
        "https://cloud.google.com/pubsub/lite/docs/reservations;"
        "https://cloud.google.com/pubsub/lite/quotas"
    )
    rows = []
    for count in args.events:
        rows.append(
            {
                "benchmark_version": "0.3.1-audit",
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "run_kind": "analytical_capacity_bound",
                "origin": "estimacion_analitica_no_medicion",
                "system": "Cloud Pub/Sub Lite (modelo oficial)",
                "run_id": f"MODEL-{uuid.uuid4()}",
                "repetition": "N/A",
                "status": "estimated_capacity_bound",
                "events_requested": count,
                "payload_bytes": args.size_bytes,
                "partitions": args.partitions,
                "replication_factor": "N/A",
                "acks": "N/A; servicio documenta resultado de publicación",
                "compression": "N/A",
                "model_publish_capacity_mib_per_s": pub_capacity,
                "model_subscribe_capacity_mib_per_s": sub_capacity,
                "model_event_storage_bytes": stored_event_bytes,
                "model_retention_capacity_events": storage_event_capacity,
                "model_retention_capacity_gib": storage_bytes / (1 << 30),
                "model_max_publish_events_per_s": round(pub_max, 6),
                "model_max_subscribe_events_per_s": round(sub_max, 6),
                "model_payload_publish_mib_per_s": round(payload_pub_mib, 6),
                "model_payload_subscribe_mib_per_s": round(payload_sub_mib, 6),
                "model_assumptions": assumptions,
                "model_sources": sources,
            }
        )
    return rows


def make_payload(sequence: int, timestamp_ns: int, size_bytes: int) -> bytes:
    if size_bytes < 16:
        raise ValueError("size-bytes debe ser >= 16 para guardar secuencia y reloj")
    header = sequence.to_bytes(8, "big") + timestamp_ns.to_bytes(8, "big")
    payload = header + (b"\xa5" * (size_bytes - len(header)))
    assert len(payload) == size_bytes
    return payload


def consume_topic(
    bootstrap: str,
    topic: str,
    group_id: str,
    run_id: str,
    expected: int,
    payload_bytes: int,
    timeout_s: float,
) -> dict[str, Any]:
    from confluent_kafka import Consumer

    init_started_ns = time.perf_counter_ns()
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": False,
        }
    )
    consumer_init_ms = (time.perf_counter_ns() - init_started_ns) / 1e6
    found: set[int] = set()
    duplicates = 0
    errors = 0
    assignment_ns = 0
    first_message_ns = 0
    last_message_ns = 0
    assignment_started_ns = time.perf_counter_ns()
    deadline = time.monotonic() + timeout_s

    def assigned(client: Any, partitions: list[Any]) -> None:
        nonlocal assignment_ns
        client.assign(partitions)
        assignment_ns = time.perf_counter_ns()

    def accept(message: Any) -> None:
        nonlocal duplicates, errors, first_message_ns, last_message_ns
        if message.error():
            errors += 1
            return
        key = message.key()
        value = message.value()
        if key is None or value is None or len(value) != payload_bytes:
            errors += 1
            return
        try:
            prefix, seq_text = key.decode("ascii").split(":", 1)
            sequence = int(seq_text)
            stored_sequence = int.from_bytes(value[:8], "big")
        except (UnicodeDecodeError, ValueError):
            errors += 1
            return
        if (
            prefix != run_id
            or sequence < 0
            or sequence >= expected
            or stored_sequence != sequence
            or value[16:] != b"\xa5" * (payload_bytes - 16)
            or int.from_bytes(value[8:16], "big") <= 0
        ):
            errors += 1
            return
        received_ns = time.perf_counter_ns()
        if not first_message_ns:
            first_message_ns = received_ns
        last_message_ns = received_ns
        if sequence in found:
            duplicates += 1
        else:
            found.add(sequence)

    try:
        consumer.subscribe([topic], on_assign=assigned)
        # Exclude group discovery and initial partition assignment from the
        # transfer interval, but time it separately.
        while assignment_ns == 0 and time.monotonic() < deadline:
            message = consumer.poll(0.25)
            if message is not None:
                accept(message)
        assignment_ms = (
            (assignment_ns - assignment_started_ns) / 1e6
            if assignment_ns
            else ""
        )
        if not assignment_ns:
            raise TimeoutError(f"partition assignment timed out for {topic}")
        while time.monotonic() < deadline:
            message = consumer.poll(0.25)
            if message is None:
                continue
            accept(message)
            if len(found) >= expected:
                break
    finally:
        consumer.close()
    if not assignment_ns:
        assignment_ms = ""
    transfer_elapsed_s = (
        max((last_message_ns - first_message_ns) / 1e9, 1e-9)
        if first_message_ns and last_message_ns
        else 0.0
    )
    post_assignment_s = (
        max((last_message_ns - assignment_ns) / 1e9, 0.0)
        if assignment_ns and last_message_ns
        else 0.0
    )
    first_message_ms = (
        (first_message_ns - assignment_ns) / 1e6
        if assignment_ns and first_message_ns
        else ""
    )
    return {
        "unique": len(found),
        "duplicates": duplicates,
        "errors": errors,
        "missing": max(expected - len(found), 0),
        "messages_per_s": len(found) / transfer_elapsed_s if transfer_elapsed_s else 0.0,
        "mib_per_s": (
            len(found) * payload_bytes / (1 << 20) / transfer_elapsed_s
            if transfer_elapsed_s
            else 0.0
        ),
        "duration_s": transfer_elapsed_s,
        "post_assignment_s": post_assignment_s,
        "consumer_init_ms": consumer_init_ms,
        "assignment_ms": assignment_ms,
        "first_message_ms": first_message_ms,
        "end_ns": last_message_ns,
    }


def kafka_run(
    args: argparse.Namespace, event_count: int, repetition: int
) -> dict[str, Any]:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    started_utc = datetime.now(timezone.utc).isoformat()
    run_id = uuid.uuid4().hex
    topic = f"bench-{run_id[:20]}"
    warmup_topic = f"warm-{run_id[:20]}"
    retention_config = {
        "retention.ms": str(args.kafka_retention_ms),
        "retention.bytes": str(args.kafka_retention_bytes),
        "cleanup.policy": "delete",
        "segment.bytes": str(args.kafka_segment_bytes),
        "segment.ms": str(args.kafka_segment_ms),
    }

    # Conservative preflight envelope: payload + 256 bytes/record for Kafka
    # record/index overhead, while preserving the configured free-space reserve.
    free_bytes = shutil.disk_usage(args.kafka_data_dir).free
    estimated_log_bytes = event_count * (args.size_bytes + 256)
    if free_bytes - estimated_log_bytes < args.reserve_disk_gib * (1 << 30):
        raise OSError(
            f"disk reserve would be crossed: free={free_bytes}, "
            f"next-run estimate={estimated_log_bytes}, reserve={args.reserve_disk_gib} GiB"
        )

    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    topic_started_ns = time.perf_counter_ns()
    topic_futures = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=1,
                replication_factor=1,
                config=retention_config,
            ),
            NewTopic(
                warmup_topic,
                num_partitions=1,
                replication_factor=1,
                config=retention_config,
            ),
        ]
    )
    for future in topic_futures.values():
        future.result(timeout=30)
    topic_creation_ms = (time.perf_counter_ns() - topic_started_ns) / 1e6

    producer_config = {
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": f"bench-producer-{run_id[:12]}",
        "acks": "all",
        "compression.type": "none",
        "enable.idempotence": True,
        "linger.ms": 0,
        "batch.size": args.batch_size_bytes,
        "max.in.flight.requests.per.connection": 5,
        "request.timeout.ms": 30000,
        "message.timeout.ms": int(args.timeout_s * 1000),
    }
    producer_init_started_ns = time.perf_counter_ns()
    producer = Producer(producer_config)
    producer_init_ms = (time.perf_counter_ns() - producer_init_started_ns) / 1e6

    metadata_started_ns = time.perf_counter_ns()
    target_metadata = producer.list_topics(topic=topic, timeout=args.timeout_s)
    warm_metadata = producer.list_topics(topic=warmup_topic, timeout=args.timeout_s)
    if topic not in target_metadata.topics or warmup_topic not in warm_metadata.topics:
        raise RuntimeError("topic metadata missing before measurement")
    if len(target_metadata.topics[topic].partitions) != 1:
        raise RuntimeError("target topic does not have exactly one partition")
    metadata_ms = (time.perf_counter_ns() - metadata_started_ns) / 1e6

    warmup_ack: list[tuple[int, Any]] = []
    warmup_started_ns = time.perf_counter_ns()
    warmup_sent_ns = time.perf_counter_ns()
    warmup_payload = make_payload(0, warmup_sent_ns, args.size_bytes)
    producer.produce(
        warmup_topic,
        key=f"warmup:{run_id}".encode("ascii"),
        value=warmup_payload,
        on_delivery=lambda err, msg: warmup_ack.append(
            (time.perf_counter_ns(), err)
        ),
    )
    warmup_deadline = time.monotonic() + args.timeout_s
    while not warmup_ack and time.monotonic() < warmup_deadline:
        producer.poll(0.05)
    if not warmup_ack:
        raise TimeoutError("warm-up confirmation timed out")
    warmup_ack_ms = (warmup_ack[0][0] - warmup_sent_ns) / 1e6
    warmup_total_ms = (time.perf_counter_ns() - warmup_started_ns) / 1e6
    if warmup_ack[0][1] is not None:
        raise RuntimeError(f"warm-up delivery failed: {warmup_ack[0][1]}")
    # Drain any additional callbacks before starting the measured run.
    producer.poll(0)

    ack_latencies_ms: list[float] = []
    producer_errors: list[str] = []
    confirmed = 0

    def delivered(err: Any, msg: Any, sent_ns: int) -> None:
        nonlocal confirmed
        callback_ns = time.perf_counter_ns()
        if err is not None:
            producer_errors.append(str(err))
        else:
            confirmed += 1
            # Application-observed latency: enqueue-to-delivery-callback.
            # It includes client queue/network/broker response/callback dispatch;
            # it is not the broker's internal processing latency.
            ack_latencies_ms.append((callback_ns - sent_ns) / 1e6)

    publish_started_ns = time.perf_counter_ns()
    for sequence in range(event_count):
        event_clock_ns = time.perf_counter_ns()
        payload = make_payload(sequence, event_clock_ns, args.size_bytes)
        key = f"{run_id}:{sequence}".encode("ascii")
        ack_start_ns = time.perf_counter_ns()
        enqueue_deadline = time.monotonic() + args.timeout_s
        enqueued = False
        while time.monotonic() < enqueue_deadline:
            try:
                producer.produce(
                    topic,
                    value=payload,
                    key=key,
                    on_delivery=lambda err, msg, sent=ack_start_ns: delivered(
                        err, msg, sent
                    ),
                )
                enqueued = True
                break
            except BufferError:
                producer.poll(0.01)
            except Exception as exc:
                producer_errors.append(f"produce: {type(exc).__name__}: {exc}")
                break
        if not enqueued and not producer_errors:
            producer_errors.append(f"enqueue timeout at sequence {sequence}")
        # Delivery callbacks are dispatched only by poll()/flush(); poll every
        # record so callbacks are serviced throughout the asynchronous send loop.
        producer.poll(0)
    pending_confirmations = producer.flush(args.timeout_s)
    publish_ended_ns = time.perf_counter_ns()
    publish_duration_s = max((publish_ended_ns - publish_started_ns) / 1e9, 1e-9)
    if pending_confirmations:
        producer_errors.append(f"{pending_confirmations} confirmation(s) pending at flush timeout")

    primary = consume_topic(
        args.bootstrap_servers,
        topic,
        f"bench-read-{run_id}",
        run_id,
        event_count,
        args.size_bytes,
        args.timeout_s,
    )
    primary_consume_end_ns = primary["end_ns"] or time.perf_counter_ns()
    retention_start_ns = time.perf_counter_ns()
    minimum_replay_ns = int(args.retention_delay_seconds * 1e9)
    remaining_delay_ns = minimum_replay_ns - (
        retention_start_ns - primary_consume_end_ns
    )
    if remaining_delay_ns > 0:
        time.sleep(remaining_delay_ns / 1e9)
    retention_read_started_ns = time.perf_counter_ns()
    retention = consume_topic(
        args.bootstrap_servers,
        topic,
        f"bench-retention-{run_id}",
        run_id,
        event_count,
        args.size_bytes,
        args.timeout_s,
    )
    retention_elapsed_from_publish_s = (
        retention_read_started_ns - publish_ended_ns
    ) / 1e9
    retention_elapsed_from_consume_s = (
        retention_read_started_ns - primary_consume_end_ns
    ) / 1e9

    long_retention: dict[str, Any] = {
        "unique": "",
        "missing": "",
        "errors": 0,
    }
    long_retention_elapsed_from_publish_s: float | str = ""
    long_retention_elapsed_from_consume_s: float | str = ""
    if args.long_retention_delay_seconds > 0:
        target_long_check_ns = primary_consume_end_ns + int(
            args.long_retention_delay_seconds * 1e9
        )
        remaining_long_ns = target_long_check_ns - time.perf_counter_ns()
        if remaining_long_ns > 0:
            time.sleep(remaining_long_ns / 1e9)
        long_read_started_ns = time.perf_counter_ns()
        long_retention = consume_topic(
            args.bootstrap_servers,
            topic,
            f"bench-retention-long-{run_id}",
            run_id,
            event_count,
            args.size_bytes,
            args.timeout_s,
        )
        long_retention_elapsed_from_publish_s = (
            long_read_started_ns - publish_ended_ns
        ) / 1e9
        long_retention_elapsed_from_consume_s = (
            long_read_started_ns - primary_consume_end_ns
        ) / 1e9

    consumer_errors = (
        primary["errors"] + retention["errors"] + long_retention["errors"]
    )
    all_errors = len(producer_errors) + consumer_errors
    valid = (
        all_errors == 0
        and pending_confirmations == 0
        and confirmed == event_count
        and len(ack_latencies_ms) == event_count
        and primary["unique"] == event_count
        and primary["missing"] == 0
        and primary["duplicates"] == 0
        and retention["unique"] == event_count
        and retention["missing"] == 0
        and retention["duplicates"] == 0
        and (
            args.long_retention_delay_seconds <= 0
            or (
                long_retention["unique"] == event_count
                and long_retention["missing"] == 0
                and long_retention["duplicates"] == 0
            )
        )
    )
    peak_rss_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    effective_config = (
        "producer: acks=all,compression=none,enable.idempotence=true,"
        f"linger.ms=0,batch.size={args.batch_size_bytes},"
        "max.in.flight.requests.per.connection=5,request.timeout.ms=30000,"
        f"message.timeout.ms={int(args.timeout_s * 1000)}; "
        "consumer: auto.offset.reset=earliest,enable.auto.commit=false,"
        "group.id unique por lectura; "
        f"topic: retention.ms={args.kafka_retention_ms},"
        f"retention.bytes={args.kafka_retention_bytes},cleanup.policy=delete,"
        f"segment.bytes={args.kafka_segment_bytes},segment.ms={args.kafka_segment_ms}"
    )
    return {
        "benchmark_version": "0.3.1-audit",
        "timestamp_utc": started_utc,
        "run_kind": "definitive" if event_count >= 10_000 else "validation_pilot",
        "origin": "medicion_real",
        "system": "Apache Kafka",
        "run_id": run_id,
        "topic": topic,
        "repetition": repetition,
        "status": "ok" if valid else "partial_or_error",
        "events_requested": event_count,
        "payload_bytes": args.size_bytes,
        "partitions": 1,
        "replication_factor": 1,
        "acks": "all",
        "compression": "none",
        "linger_ms": 0,
        "batch_size_bytes": args.batch_size_bytes,
        "idempotence": True,
        "max_in_flight": 5,
        "retention_ms": args.kafka_retention_ms,
        "retention_bytes": args.kafka_retention_bytes,
        "cleanup_policy": "delete",
        "segment_bytes": args.kafka_segment_bytes,
        "segment_ms": args.kafka_segment_ms,
        "topic_creation_ms": topic_creation_ms,
        "producer_init_ms": producer_init_ms,
        "metadata_ms": metadata_ms,
        "warmup_topic": warmup_topic,
        "warmup_total_ms": warmup_total_ms,
        "warmup_ack_ms": warmup_ack_ms,
        "consumer_init_ms": primary["consumer_init_ms"],
        "consumer_assignment_ms": primary["assignment_ms"],
        "consumer_first_message_ms": primary["first_message_ms"],
        "publish_duration_s": publish_duration_s,
        "consumer_duration_s": primary["duration_s"],
        "consumer_post_assignment_s": primary["post_assignment_s"],
        "publish_messages_per_s": confirmed / publish_duration_s,
        "publish_mib_per_s": (
            confirmed * args.size_bytes / (1 << 20) / publish_duration_s
        ),
        "consume_messages_per_s": primary["messages_per_s"],
        "consume_mib_per_s": primary["mib_per_s"],
        "ack_latency_p50_ms": percentile(ack_latencies_ms, 50),
        "ack_latency_p95_ms": percentile(ack_latencies_ms, 95),
        "ack_latency_p99_ms": percentile(ack_latencies_ms, 99),
        "confirmed_events": confirmed,
        "unconfirmed_events": max(event_count - confirmed, 0),
        "unique_recovered": primary["unique"],
        "primary_recovery_percent": primary["unique"] / event_count * 100,
        "missing_events": primary["missing"],
        "duplicate_events": primary["duplicates"],
        "error_count": all_errors,
        "errors_detail": "; ".join(producer_errors[:20]),
        "retention_delay_s": args.retention_delay_seconds,
        "retention_elapsed_from_publish_s": retention_elapsed_from_publish_s,
        "retention_elapsed_from_initial_consume_s": retention_elapsed_from_consume_s,
        "retention_unique_recovered": retention["unique"],
        "retention_missing_events": retention["missing"],
        "long_retention_delay_s": args.long_retention_delay_seconds,
        "long_retention_elapsed_from_publish_s": long_retention_elapsed_from_publish_s,
        "long_retention_elapsed_from_initial_consume_s": long_retention_elapsed_from_consume_s,
        "long_retention_unique_recovered": long_retention["unique"],
        "long_retention_missing_events": long_retention["missing"],
        "peak_rss_mib": peak_rss_mib,
        "model_assumptions": (
            "ack latency is application-observed enqueue-to-delivery-callback; "
            "not broker-internal latency; publication transfer excludes producer "
            "construction, metadata and warm-up; consumer transfer starts after "
            "partition assignment"
        ),
        "effective_config": effective_config,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("kafka", "pubsublite-analytical", "summarize"),
        default="kafka",
    )
    parser.add_argument("--events", type=int, nargs="+", default=[1000])
    parser.add_argument("--size-bytes", type=int, default=1024)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--retention-delay-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-s", type=float, default=300.0)
    parser.add_argument("--output", type=Path, default=Path("results.csv"))
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--storage-gib", type=int, default=30)
    parser.add_argument("--model-publish-mib-per-s", type=float, default=4.0)
    parser.add_argument("--model-subscribe-mib-per-s", type=float, default=4.0)
    parser.add_argument("--batch-size-bytes", type=int, default=16384)
    parser.add_argument("--kafka-retention-ms", type=int, default=604800000)
    parser.add_argument("--kafka-retention-bytes", type=int, default=-1)
    parser.add_argument("--kafka-segment-bytes", type=int, default=1073741824)
    parser.add_argument("--kafka-segment-ms", type=int, default=604800000)
    parser.add_argument("--kafka-data-dir", type=Path, default=Path("data/kafka/logs"))
    parser.add_argument("--reserve-disk-gib", type=float, default=10.0)
    parser.add_argument("--long-retention-delay-seconds", type=float, default=30.0)
    args = parser.parse_args()
    if any(count <= 0 for count in args.events):
        parser.error("events deben ser enteros positivos")
    if (
        args.repetitions < 1
        or args.timeout_s <= 0
        or args.retention_delay_seconds < 0
        or args.long_retention_delay_seconds < 0
        or args.batch_size_bytes <= 0
        or args.reserve_disk_gib < 0
    ):
        parser.error("parámetros de timeout, retención, batch y reserva inválidos")
    return args


def main() -> int:
    args = parse_args()
    try:
        if args.mode == "summarize":
            rows = statistical_summary(args.output)
            for row in rows:
                print(
                    f'{row["events_requested"]}: mean={row["throughput_mean_messages_per_s"]:.3f} '
                    f'msg/s, sample_sd={row["throughput_sample_sd_messages_per_s"]:.3f}, '
                    f'CV={row["throughput_cv_percent"]:.2f}%'
                )
            return 0
        if args.mode == "pubsublite-analytical":
            rows = analytical_model(args)
            append_rows(args.output, rows)
            for row in rows:
                print(
                    f'{row["system"]}: {row["events_requested"]} eventos; '
                    f'límite analítico de publicación={row["model_max_publish_events_per_s"]} msg/s; '
                    "sin latencia ni pérdida estimadas"
                )
            return 0
        rows = []
        event_counts = list(args.events)
        for repetition in range(1, args.repetitions + 1):
            for event_count in event_counts:
                try:
                    row = kafka_run(args, event_count, repetition)
                except Exception as exc:
                    row = {
                        "origin": "medicion_real",
                        "system": "Apache Kafka",
                        "run_id": f"FAILED-{uuid.uuid4()}",
                        "repetition": repetition,
                        "status": "failed",
                        "events_requested": event_count,
                        "payload_bytes": args.size_bytes,
                        "partitions": 1,
                        "replication_factor": 1,
                        "acks": "all",
                        "compression": "none",
                        "error_count": 1,
                        "retention_delay_s": args.retention_delay_seconds,
                        "model_assumptions": f"fallo de ejecución: {type(exc).__name__}: {exc}",
                    }
                rows.append(row)
                print(
                    f'{row["status"]}: {row["events_requested"]} eventos; '
                    f'throughput={row.get("publish_messages_per_s", "")}; '
                    f'recuperados={row.get("unique_recovered", "")}; '
                    f'retenidos={row.get("retention_unique_recovered", "")}'
                )
                if row["status"] != "ok":
                    print("Ejecución detenida: el resultado no superó validaciones.")
                    break
            if rows and rows[-1]["status"] != "ok":
                break
        append_rows(args.output, rows)
        return 0 if all(row["status"] == "ok" for row in rows) else 2
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
