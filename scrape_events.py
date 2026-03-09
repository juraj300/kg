from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests


def _log(message: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"[{ts}] [scrape] {message}")


def _normalize_venue_url(page_url: str, discover_by: str) -> str:
    url = page_url.strip()
    if discover_by != "venue":
        return url
    if "facebook.com" in url and "/events" not in url:
        return url.rstrip("/") + "/events"
    return url


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _to_iso(value: Any) -> str | None:
    dt = _parse_dt(value)
    if not dt:
        return None
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _pick(event: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        value = event.get(key)
        if value not in (None, ""):
            return value
    return None


def _extract_fb_id(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"/events/(\d+)", url)
    return match.group(1) if match else None


def _guess_city(location: Any) -> str | None:
    if not location:
        return None
    if isinstance(location, dict):
        return (
            location.get("city")
            or location.get("locality")
            or location.get("addressLocality")
            or None
        )
    text = str(location)
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[-2]
    return parts[-1] if parts else None


def _bright_headers() -> dict[str, str]:
    token = os.getenv("BRIGHTDATA_API_TOKEN", "").strip()
    if not token or token.upper().startswith("YOUR_"):
        raise ValueError("Missing BRIGHTDATA_API_TOKEN in environment.")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def fetch_events_from_brightdata(page_url: str) -> list[dict[str, Any]]:
    """
    Fetch events from Bright Data dataset API.
    Required env vars:
    - BRIGHTDATA_API_TOKEN
    - BRIGHTDATA_DATASET_ID
    Optional env vars:
    - BRIGHTDATA_API_BASE (default: https://api.brightdata.com/datasets/v3)
    - BRIGHTDATA_DISCOVER_BY (default: venue)
    - BRIGHTDATA_UPCOMING_ONLY (default: true)
    - BRIGHTDATA_POLL_SECONDS (default: 2)
    - BRIGHTDATA_TIMEOUT_SECONDS (default: 120)
    """
    dataset_id = os.getenv("BRIGHTDATA_DATASET_ID", "").strip()
    if not dataset_id or dataset_id.upper().startswith("YOUR_"):
        raise ValueError("Missing BRIGHTDATA_DATASET_ID in environment.")

    base = os.getenv("BRIGHTDATA_API_BASE", "https://api.brightdata.com/datasets/v3").rstrip("/")
    discover_by = os.getenv("BRIGHTDATA_DISCOVER_BY", "venue").strip()
    upcoming_only = os.getenv("BRIGHTDATA_UPCOMING_ONLY", "true").strip().lower() in {"1", "true", "yes"}
    poll_seconds = float(os.getenv("BRIGHTDATA_POLL_SECONDS", "2"))
    timeout_seconds = int(os.getenv("BRIGHTDATA_TIMEOUT_SECONDS", "120"))
    headers = _bright_headers()
    _log(
        "Starting Bright Data request "
        f"(dataset_id={dataset_id}, discover_by={discover_by}, upcoming_only={upcoming_only})"
    )

    normalized_url = _normalize_venue_url(page_url, discover_by)
    if normalized_url != page_url:
        _log(f"Normalized venue URL to '{normalized_url}'")

    scrape_url = f"{base}/scrape"
    params = {
        "dataset_id": dataset_id,
        "notify": "false",
        "include_errors": "true",
        "type": "discover_new",
        "discover_by": discover_by,
    }
    payload_primary = {"input": [{"url": normalized_url, "upcoming_events_only": upcoming_only}]}
    payload_fallback = [{"url": normalized_url, "upcoming_events_only": upcoming_only}]

    scrape_resp = requests.post(
        scrape_url,
        headers=headers,
        params=params,
        json=payload_primary,
        timeout=30,
    )
    _log(f"POST {scrape_url} (payload=input-wrapper) -> HTTP {scrape_resp.status_code}")
    if scrape_resp.status_code >= 400:
        _log(f"Bright Data error body: {scrape_resp.text[:500]}")
        _log("Retrying scrape with list payload format")
        scrape_resp = requests.post(
            scrape_url,
            headers=headers,
            params=params,
            json=payload_fallback,
            timeout=30,
        )
        _log(f"POST {scrape_url} (payload=list) -> HTTP {scrape_resp.status_code}")
        if scrape_resp.status_code >= 400:
            _log(f"Bright Data error body (retry): {scrape_resp.text[:500]}")
    scrape_resp.raise_for_status()
    scrape_data = scrape_resp.json()

    # In synchronous mode Bright Data may return the records directly.
    if isinstance(scrape_data, list):
        _log(f"Synchronous response with {len(scrape_data)} records")
        return scrape_data
    if isinstance(scrape_data, dict):
        for key in ("items", "events", "data", "results"):
            if isinstance(scrape_data.get(key), list):
                _log(f"Synchronous response key='{key}' with {len(scrape_data[key])} records")
                return scrape_data[key]

    # In asynchronous mode we get snapshot id and then poll/download.
    snapshot_id = None
    if isinstance(scrape_data, dict):
        snapshot_id = scrape_data.get("snapshot_id") or scrape_data.get("id")
    if not snapshot_id:
        raise RuntimeError(f"Unexpected scrape response: {scrape_data}")
    _log(f"Asynchronous mode, snapshot_id={snapshot_id}")

    progress_url = f"{base}/progress/{snapshot_id}"
    snapshot_url = f"{base}/snapshot/{snapshot_id}"
    started = time.time()

    while True:
        progress_resp = requests.get(progress_url, headers=headers, timeout=30)
        progress_resp.raise_for_status()
        progress = progress_resp.json()
        status = str(progress.get("status", "")).lower()
        _log(f"Snapshot progress status='{status}'")
        if status in {"ready", "completed", "done"}:
            break
        if status in {"failed", "error", "aborted"}:
            raise RuntimeError(f"Bright Data scrape failed. Status: {status}. Response: {progress}")
        if time.time() - started > timeout_seconds:
            raise TimeoutError("Timed out waiting for Bright Data snapshot to be ready.")
        time.sleep(poll_seconds)

    data_resp = requests.get(snapshot_url, headers=headers, params={"format": "json"}, timeout=30)
    _log(f"GET {snapshot_url}?format=json -> HTTP {data_resp.status_code}")
    data_resp.raise_for_status()
    payload = data_resp.json()
    if isinstance(payload, list):
        _log(f"Downloaded {len(payload)} records from snapshot")
        return payload
    if isinstance(payload, dict):
        for key in ("items", "events", "data", "results"):
            if isinstance(payload.get(key), list):
                _log(f"Downloaded key='{key}' with {len(payload[key])} records")
                return payload[key]
    _log("No records in snapshot payload")
    return []


def map_events_to_kamgo(events: list[dict[str, Any]], scrapped_at: str) -> list[dict[str, Any]]:
    _log(f"Mapping {len(events)} raw records to Kamgo schema")
    mapped: list[dict[str, Any]] = []
    for event in events:
        fb_url = _pick(event, ["event_url", "url", "link", "eventUrl"])
        location = _pick(event, ["location", "address", "place", "venue", "location_text"])
        start_raw = _pick(event, ["start_time", "startAt", "start_date", "date", "start"])
        finish_raw = _pick(event, ["end_time", "finishAt", "end_date", "finish", "end"])
        ticket_url = _pick(event, ["ticket_url", "ticketUrl", "tickets", "ticket_link"])
        image_url = _pick(event, ["image", "image_url", "imageUrl", "cover_image"])
        name = _pick(event, ["name", "title", "event_name"])
        description = _pick(event, ["description", "details", "about"])
        place_name = _pick(event, ["place_name", "placeName", "venue_name", "location_name"])

        mapped.append(
            {
                "fbId": _extract_fb_id(str(fb_url) if fb_url else None),
                "fbUrl": fb_url,
                "name": name,
                "description": description,
                "placeName": place_name or (location.get("name") if isinstance(location, dict) else None),
                "city": _guess_city(location),
                "startAt": _to_iso(start_raw),
                "finishAt": _to_iso(finish_raw),
                "imageUrl": image_url,
                "ticketUrl": ticket_url,
                "scrappedAt": scrapped_at,
            }
        )
    _log(f"Mapped records: {len(mapped)}")
    return mapped


def estimate_activity(events: list[dict[str, Any]]) -> dict[str, Any]:
    _log(f"Estimating activity from {len(events)} events")
    start_dates = []
    for event in events:
        dt = _parse_dt(event.get("startAt"))
        if dt:
            start_dates.append(dt)

    start_dates = sorted(start_dates)
    events_found = len(events)
    events_per_30_days = 0.0
    avg_days_between_events = None

    if start_dates:
        span_days = max((start_dates[-1] - start_dates[0]).days, 1)
        events_per_30_days = len(start_dates) * 30.0 / span_days

    if len(start_dates) >= 2:
        gaps = []
        for i in range(1, len(start_dates)):
            gaps.append((start_dates[i] - start_dates[i - 1]).total_seconds() / 86400.0)
        avg_days_between_events = round(sum(gaps) / len(gaps), 2)

    if events_per_30_days > 10:
        level = "high"
    elif events_per_30_days >= 3:
        level = "medium"
    else:
        level = "low"

    result = {
        "events_found": events_found,
        "avg_days_between_events": avg_days_between_events,
        "activity_level": level,
    }
    _log(
        "Activity result: "
        f"events_found={result['events_found']}, "
        f"avg_days_between_events={result['avg_days_between_events']}, "
        f"activity_level={result['activity_level']}"
    )
    return result
