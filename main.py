from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from classify_events import classify_events
from scrape_events import estimate_activity, fetch_events_from_brightdata, map_events_to_kamgo


def load_dotenv(path: str = ".env") -> None:
    """Tiny .env loader so we can keep dependencies minimal."""
    env_path = Path(path)
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        clean = line.strip()
        if not clean or clean.startswith("#") or "=" not in clean:
            continue
        key, value = clean.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def write_json(path: str, payload: object) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def log(message: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"[{ts}] {message}")


def run() -> None:
    log("Pipeline start")
    load_dotenv()
    log("Loaded .env")

    page_url = os.getenv("FB_PAGE_URL", "https://www.facebook.com/LIFEmusicclubPB")
    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    log(f"Input page_url={page_url}")

    log("Step 1/5: Fetching events from Bright Data")
    raw_events = fetch_events_from_brightdata(page_url)
    log(f"Bright Data returned {len(raw_events)} raw records")

    log("Step 2/5: Mapping records to Kamgo schema")
    mapped_events = map_events_to_kamgo(raw_events, scrapped_at=scraped_at)
    log(f"Mapped {len(mapped_events)} events")

    log("Step 3/5: Estimating activity")
    activity = estimate_activity(mapped_events)
    log(
        "Activity stats: "
        f"events_found={activity['events_found']}, "
        f"avg_days_between_events={activity['avg_days_between_events']}, "
        f"activity_level={activity['activity_level']}"
    )

    log("Step 4/5: Classifying events with Gemini")
    classified_events = classify_events(mapped_events)
    log(f"Classified {len(classified_events)} events")

    log("Step 5/5: Writing output files")
    write_json("events_raw.json", mapped_events)
    write_json("events_classified.json", classified_events)
    log("Saved events_raw.json and events_classified.json")

    print(f"Events discovered: {activity['events_found']}")
    print(f"Estimated activity: {activity['activity_level']}")
    print(f"Events classified: {len(classified_events)}")
    log("Pipeline done")


if __name__ == "__main__":
    run()
