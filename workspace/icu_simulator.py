"""
ICU live-data simulator: publishes NEW synthetic readings to Kafka every time it runs.

Run it from Airflow (e.g. every 15 minutes). Each run emits one reading per patient for every
5-minute slot since the previous run (3 readings per patient when the DAG runs every 15 minutes),
so downstream layers see data that looks like a bedside monitor sending a reading every 5 minutes.

Realism:
- Each patient has a healthy baseline; vitals follow a small random walk around it.
- Occasionally a patient starts a deterioration episode (SpO2 falls, heart rate and temperature rise),
  becomes Warning, then Critical, and later recovers -> exercises scoring and alerts.
- Occasionally a device goes silent for a while -> exercises the stale-device detection.
- Small rates of missing values, Fahrenheit readings, missing units and duplicate messages
  -> exercise silver cleaning, unit normalization and deduplication.

State (current vitals per patient, last emitted slot) is stored in STATE_PATH, so runs continue
from where the previous one stopped. If a run fails, the state is not saved and the retry re-emits
the same slots (silver deduplicates).

The data is SYNTHETIC. Patient ids and devices are taken from the source CSV so they match the
historical data; with no CSV, patients 1000..(1000+NUM_PATIENTS-1) are generated.
"""
import csv
import json
import logging
import os
import random
import sys
import traceback
from datetime import datetime, timedelta, timezone

# ==========================================
# Configuration (environment variables)
# ==========================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "patient_vitals")
DELIVERY_TIMEOUT_SEC = float(os.getenv("DELIVERY_TIMEOUT_SEC", "30"))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.getenv("CSV_PATH", os.path.join(SCRIPT_DIR, "data", "icu_vitals.csv"))
STATE_PATH = os.getenv("STATE_PATH", os.path.join(SCRIPT_DIR, "simulator_state.json"))
LOG_DIR = os.getenv("LOG_DIR", os.path.join(SCRIPT_DIR, "logs"))
NUM_PATIENTS = int(os.getenv("NUM_PATIENTS", "60"))  # only used when no CSV is available

READING_INTERVAL_MIN = int(os.getenv("READING_INTERVAL_MIN", "5"))
DEFAULT_STEPS = int(os.getenv("DEFAULT_STEPS", "3"))        # slots emitted on the very first run
MAX_CATCHUP_STEPS = int(os.getenv("MAX_CATCHUP_STEPS", "12"))  # cap after a long pause (1 hour)

EPISODE_PROB = float(os.getenv("EPISODE_PROB", "0.002"))    # per patient per slot
MAX_EPISODES = int(os.getenv("MAX_EPISODES", "4"))          # concurrent deterioration episodes
SILENT_PROB = float(os.getenv("SILENT_PROB", "0.002"))      # per patient per slot
P_MISSING = float(os.getenv("P_MISSING", "0.02"))           # a vital is missing
P_FAHRENHEIT = float(os.getenv("P_FAHRENHEIT", "0.03"))     # temperature sent in F
P_NO_UNIT = float(os.getenv("P_NO_UNIT", "0.01"))           # temp_unit missing
P_DUPLICATE = float(os.getenv("P_DUPLICATE", "0.01"))       # message sent twice

SEED = os.getenv("SEED")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"


# ==========================================
# Helpers
# ==========================================
def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [Simulator] - %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(LOG_DIR, "icu_simulator.log"), encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def floor_to_grid(dt, minutes):
    return dt.replace(second=0, microsecond=0, minute=(dt.minute // minutes) * minutes)


def clamp(value, low, high):
    return max(low, min(high, value))


# ==========================================
# State
# ==========================================
def load_patients_from_csv(path):
    """patient_id -> device_id (last device seen) from the source CSV."""
    patients = {}
    if not os.path.exists(path):
        return patients
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pid = (row.get("patient_id") or "").strip()
            dev = (row.get("device_id") or "").strip()
            if pid and dev:
                patients[pid] = dev
    return patients


def new_patient(device_id, rng):
    base_hr = clamp(rng.gauss(80, 9), 58, 100)
    base_spo2 = clamp(rng.gauss(97, 1.1), 94.5, 99.5)
    base_temp = clamp(rng.gauss(36.9, 0.3), 36.2, 37.6)
    return {
        "device_id": device_id,
        "base_hr": base_hr, "base_spo2": base_spo2, "base_temp": base_temp,
        "hr": base_hr, "spo2": base_spo2, "temp": base_temp,
        "episode_left": 0, "silent_left": 0,
    }


def init_state(rng):
    devices = load_patients_from_csv(CSV_PATH)
    if not devices:
        logging.warning("No patients found in %s; generating %d synthetic patients", CSV_PATH, NUM_PATIENTS)
        letters = ["Monitor_A", "Monitor_B", "Monitor_C"]
        devices = {str(1000 + i): letters[i % 3] for i in range(NUM_PATIENTS)}
    logging.info("Initializing simulator state for %d patients", len(devices))
    return {"last_ts": None, "patients": {pid: new_patient(dev, rng) for pid, dev in devices.items()}}


def load_state(rng):
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return init_state(rng)


def save_state(state):
    tmp = STATE_PATH + ".tmp"
    os.makedirs(os.path.dirname(os.path.abspath(STATE_PATH)), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_PATH)


# ==========================================
# Simulation
# ==========================================
def plan_timestamps(last_ts_iso, now, interval_min=READING_INTERVAL_MIN,
                    default_steps=DEFAULT_STEPS, max_steps=MAX_CATCHUP_STEPS):
    """5-minute slots to emit: from the slot after the last emitted one up to the current slot."""
    interval = timedelta(minutes=interval_min)
    now_slot = floor_to_grid(now, interval_min)
    if last_ts_iso:
        first = parse_iso(last_ts_iso) + interval
    else:
        first = now_slot - (default_steps - 1) * interval
    first = max(first, now_slot - (max_steps - 1) * interval)

    slots = []
    t = first
    while t <= now_slot:
        slots.append(t)
        t += interval
    return slots


def step_patient(p, rng, active_episodes):
    """Advance one patient by one slot. Returns False when the device is silent (no reading)."""
    if p["silent_left"] > 0:
        p["silent_left"] -= 1
        return False

    if p["episode_left"] == 0 and active_episodes < MAX_EPISODES and rng.random() < EPISODE_PROB:
        p["episode_left"] = rng.randint(8, 14)

    if p["episode_left"] > 0:  # deterioration: SpO2 down, HR and temperature up
        p["hr"] += rng.uniform(2.0, 4.0)
        p["spo2"] -= rng.uniform(0.6, 1.4)
        p["temp"] += rng.uniform(0.05, 0.15)
        p["episode_left"] -= 1
    else:  # mean reversion with small noise (also gives recovery after an episode)
        p["hr"] += (p["base_hr"] - p["hr"]) * 0.35 + rng.gauss(0, 2.0)
        p["spo2"] += (p["base_spo2"] - p["spo2"]) * 0.35 + rng.gauss(0, 0.3)
        p["temp"] += (p["base_temp"] - p["temp"]) * 0.35 + rng.gauss(0, 0.05)

    p["hr"] = clamp(p["hr"], 40, 165)
    p["spo2"] = clamp(p["spo2"], 82, 100)
    p["temp"] = clamp(p["temp"], 35.5, 40.5)

    if rng.random() < SILENT_PROB:
        p["silent_left"] = rng.randint(5, 8)  # goes silent after this reading
    return True


def build_event(pid, p, ts, rng):
    hr = round(p["hr"])
    spo2 = round(p["spo2"], 1)
    temp = p["temp"]

    if rng.random() < P_MISSING:  # drop one vital
        missing = rng.choice(["hr", "spo2", "temp"])
        if missing == "hr":
            hr = None
        elif missing == "spo2":
            spo2 = None
        else:
            temp = None

    unit = "C"
    if temp is not None:
        if rng.random() < P_FAHRENHEIT:
            temp, unit = round(temp * 9 / 5 + 32, 1), "F"
        else:
            temp = round(temp, 2)
    if rng.random() < P_NO_UNIT:
        unit = None

    return {
        "patient_id": pid,
        "device_id": p["device_id"],
        "heart_rate": float(hr) if hr is not None else None,
        "spo2": spo2,
        "temperature": temp,
        "temp_unit": unit,
        "event_timestamp": iso(ts),
        "ingestion_timestamp": iso(datetime.now(timezone.utc)),
    }


def generate(state, slots, rng):
    """Generate events for all slots, mutating the state. Returns (events, stats)."""
    events = []
    silent_patient_slots = 0
    for ts in slots:
        active = sum(1 for p in state["patients"].values() if p["episode_left"] > 0)
        for pid, p in state["patients"].items():
            before = p["episode_left"]
            sent = step_patient(p, rng, active)
            after = p["episode_left"]
            if before == 0 and after > 0:      # a new episode started
                active += 1
            elif before > 0 and after == 0:    # an episode ended
                active -= 1

            if not sent:
                silent_patient_slots += 1
                continue

            event = build_event(pid, p, ts, rng)
            events.append(event)
            if rng.random() < P_DUPLICATE:
                events.append(dict(event))

    stats = {
        "slots": len(slots),
        "events": len(events),
        "patients_in_episode": sum(1 for p in state["patients"].values() if p["episode_left"] > 0),
        "silent_devices": sum(1 for p in state["patients"].values() if p["silent_left"] > 0),
        "silent_patient_slots": silent_patient_slots,
        "min_spo2": min((e["spo2"] for e in events if e["spo2"] is not None), default=None),
    }
    return events, stats


# ==========================================
# Kafka
# ==========================================
def publish(events):
    from kafka import KafkaProducer  # imported here so the generator can be tested without Kafka

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        enable_idempotence=True,
    )
    try:
        futures = [producer.send(TOPIC_NAME, key=e["patient_id"], value=e) for e in events]
        producer.flush(timeout=DELIVERY_TIMEOUT_SEC)
        for f in futures:
            f.get(timeout=DELIVERY_TIMEOUT_SEC)  # raises if any delivery failed
    finally:
        producer.close(timeout=DELIVERY_TIMEOUT_SEC)


# ==========================================
# Main
# ==========================================
def main():
    setup_logging()
    try:
        rng = random.Random(int(SEED)) if SEED else random.Random()
        state = load_state(rng)
        slots = plan_timestamps(state.get("last_ts"), datetime.now(timezone.utc))

        if not slots:
            logging.info("No new 5-minute slot since the last run; nothing to emit.")
            return

        events, stats = generate(state, slots, rng)
        logging.info(
            "Slots %s -> %s | events: %d | patients in deterioration episode: %d | silent devices: %d | min SpO2: %s",
            iso(slots[0]), iso(slots[-1]), stats["events"], stats["patients_in_episode"],
            stats["silent_devices"], stats["min_spo2"],
        )

        if DRY_RUN:
            logging.info("DRY_RUN=true: nothing sent, state not saved. Sample: %s", events[:2])
            return

        publish(events)
        state["last_ts"] = iso(slots[-1])
        save_state(state)  # only after every message was delivered
        logging.info("Published %d events to topic %s and saved state to %s", len(events), TOPIC_NAME, STATE_PATH)

    except Exception:
        logging.error("Simulator failed")
        logging.error(traceback.format_exc())
        raise  # non-zero exit so Airflow marks the task as failed


if __name__ == "__main__":
    main()
