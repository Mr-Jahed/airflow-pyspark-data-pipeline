import json
import os
from datetime import date

TRACKER_FILE = "data/incremental_tracker.json"

def _load() -> dict:
    if not os.path.exists(TRACKER_FILE):
        return {"processed_dates": []}
    with open(TRACKER_FILE, "r") as f:
        return json.load(f)

def _save(state: dict):
    os.makedirs(os.path.dirname(TRACKER_FILE), exist_ok=True)
    with open(TRACKER_FILE, "w") as f:
        json.dump(state, f, indent=2)

def is_already_processed(run_date: str) -> bool:
    return run_date in _load()["processed_dates"]

def mark_as_processed(run_date: str):
    state = _load()
    if run_date not in state["processed_dates"]:
        state["processed_dates"].append(run_date)
        _save(state)

def get_last_processed_date() -> str | None:
    dates = _load()["processed_dates"]
    return max(dates) if dates else None
