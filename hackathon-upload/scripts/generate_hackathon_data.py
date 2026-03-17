#!/usr/bin/env python3
"""
Hackathon: Lime Fleet Ops - Data Generation Script

Generates a JSONL file of scooter ride events (pickups/dropoffs) across Seattle zones.
Produces ~24 hours of steady-state data with an anomalous dropoff surge in the
"Seattle Center" zone (simulating a Kraken game at Climate Pledge Arena).

Usage:
    uv run generate_hackathon_data
    uv run generate_hackathon_data --output assets/hackathon/data/ride_events.jsonl
"""

import argparse
import json
import random
import sys
import time
import uuid
from pathlib import Path

# Seattle zones where Lime operates
SEATTLE_ZONES = [
    "Capitol Hill",
    "Downtown",
    "South Lake Union",
    "University District",
    "Pioneer Square",
    "Fremont",
    "Ballard",
    "Seattle Center",  # Climate Pledge Arena zone - surge target
]

SURGE_ZONE = "Seattle Center"

# Vehicle ID pool
VEHICLE_PREFIX = "LME"

WINDOW_SIZE_S = 5 * 60  # 5-minute windows
NUM_WINDOWS = 288        # 24 hours of data
NUM_EXTRA_WINDOWS = 5    # Extra windows after surge to advance watermark
SURGE_WINDOW_START = 287 # Must be AFTER minTrainingSize (286) so anomaly detection evaluates it
SURGE_WINDOW_END = 291   # 4 windows of surge (~20 minutes of heavy dropoffs)


def generate_vehicle_id():
    return f"{VEHICLE_PREFIX}-{random.randint(10000, 99999)}"


def generate_rider_email():
    first_names = [
        "alex", "jordan", "casey", "taylor", "morgan", "riley", "quinn",
        "avery", "drew", "blake", "sam", "chris", "pat", "jamie", "skyler",
        "emma", "liam", "olivia", "noah", "ava", "sophia", "jackson",
        "mia", "lucas", "harper", "ethan", "ella", "mason", "aria", "logan",
    ]
    last_names = [
        "smith", "johnson", "chen", "patel", "kim", "garcia", "wong",
        "nguyen", "lee", "martinez", "anderson", "thomas", "jackson",
        "white", "harris", "clark", "lewis", "walker", "hall", "young",
    ]
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]
    return f"{random.choice(first_names)}.{random.choice(last_names)}@{random.choice(domains)}"


def generate_events_for_window(window_index, base_ts_ms):
    """Generate ride events for a single 5-minute window."""
    events = []
    window_start_ms = base_ts_ms + (window_index * WINDOW_SIZE_S * 1000)

    is_surge_window = SURGE_WINDOW_START <= window_index < SURGE_WINDOW_END

    for zone in SEATTLE_ZONES:
        # Steady-state: 3-8 events per zone per window
        num_events = random.randint(3, 8)

        # During surge: Seattle Center gets 35-55 dropoffs
        if is_surge_window and zone == SURGE_ZONE:
            num_events = random.randint(35, 55)

        for _ in range(num_events):
            ts_offset_ms = random.randint(0, WINDOW_SIZE_S * 1000 - 1)
            event_ts = window_start_ms + ts_offset_ms

            # Determine event type
            if is_surge_window and zone == SURGE_ZONE:
                # Surge is predominantly dropoffs (people arriving at arena)
                event_type = random.choices(["dropoff", "pickup"], weights=[0.9, 0.1])[0]
            else:
                event_type = random.choices(["dropoff", "pickup"], weights=[0.5, 0.5])[0]

            # Battery level: lower on dropoffs (ride just happened), higher on pickups
            if event_type == "dropoff":
                battery_level = random.randint(5, 65)
            else:
                battery_level = random.randint(30, 95)

            # During surge, batteries are extra drained (longer rides to arena)
            if is_surge_window and zone == SURGE_ZONE and event_type == "dropoff":
                battery_level = random.randint(3, 35)

            # Pick origin/destination zone
            other_zones = [z for z in SEATTLE_ZONES if z != zone]
            if event_type == "dropoff":
                origin_zone = random.choice(other_zones)
                destination_zone = zone
            else:
                origin_zone = zone
                destination_zone = random.choice(other_zones)

            ride_duration = round(random.uniform(3.0, 25.0), 1)
            ride_cost = round(random.uniform(1.50, 12.00), 2)

            event = {
                "event_id": f"EVT-{uuid.uuid4().hex[:12].upper()}",
                "rider_email": generate_rider_email(),
                "event_type": event_type,
                "zone": zone,
                "origin_zone": origin_zone,
                "destination_zone": destination_zone,
                "vehicle_id": generate_vehicle_id(),
                "battery_level": battery_level,
                "ride_duration_minutes": ride_duration,
                "ride_cost": ride_cost,
                "event_ts": event_ts,
            }
            events.append(event)

    return events


def main():
    parser = argparse.ArgumentParser(
        description="Generate Lime ride events data for hackathon"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=Path("assets/hackathon/data/ride_events.jsonl"),
        help="Output JSONL file path",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility",
    )
    args = parser.parse_args()

    random.seed(args.seed)

    # Base timestamp: 24 hours ago aligned to 5-min boundary
    now_ms = int(time.time() * 1000)
    aligned_now = (now_ms // (WINDOW_SIZE_S * 1000)) * (WINDOW_SIZE_S * 1000)
    base_ts_ms = aligned_now - (NUM_WINDOWS * WINDOW_SIZE_S * 1000)

    print(f"Generating {NUM_WINDOWS} windows (24h) of ride events...")
    print(f"Surge zone: {SURGE_ZONE} (windows {SURGE_WINDOW_START}-{SURGE_WINDOW_END})")
    print(f"Zones: {', '.join(SEATTLE_ZONES)}")

    total_windows = SURGE_WINDOW_END + NUM_EXTRA_WINDOWS
    all_events = []
    for w in range(total_windows):
        events = generate_events_for_window(w, base_ts_ms)
        all_events.extend(events)

    # Sort by timestamp
    all_events.sort(key=lambda e: e["event_ts"])

    # Write output
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        for event in all_events:
            f.write(json.dumps(event) + "\n")

    # Stats
    total = len(all_events)
    surge_events = [e for e in all_events if e["zone"] == SURGE_ZONE
                    and SURGE_WINDOW_START <= (e["event_ts"] - base_ts_ms) // (WINDOW_SIZE_S * 1000) < SURGE_WINDOW_END]
    surge_dropoffs = [e for e in surge_events if e["event_type"] == "dropoff"]

    print(f"\nGenerated {total} total events")
    print(f"Surge events in {SURGE_ZONE}: {len(surge_events)} ({len(surge_dropoffs)} dropoffs)")
    print(f"Average battery level in surge dropoffs: {sum(e['battery_level'] for e in surge_dropoffs) / max(len(surge_dropoffs), 1):.0f}%")
    print(f"Written to: {args.output}")


if __name__ == "__main__":
    main()
