from __future__ import annotations

import copy
import json
import os
import re
from typing import Any

import requests

CATEGORIES = ["music", "culture", "sports", "education", "business", "kids", "nightlife"]


def _log(message: str) -> None:
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"[{ts}] [classify] {message}")


def _extract_json(text: str) -> dict[str, Any]:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return {}
    return {}


def classify_event(event: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key or api_key.upper().startswith("YOUR_"):
        _log("Missing GEMINI_API_KEY, returning unknown category")
        return {"category": "unknown", "confidence": 0.0}

    model = os.getenv("GEMINI_MODEL", "gemma-3-27b-it").strip()
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    _log(f"Calling Gemini model='{model}' for event name='{event.get('name')}'")

    prompt = (
        "You are classifying events for a city events platform.\n"
        f"Allowed categories: {', '.join(CATEGORIES)}.\n"
        "Return only valid JSON with keys: category, confidence.\n"
        "confidence must be a float between 0 and 1.\n\n"
        f"name: {event.get('name')}\n"
        f"description: {event.get('description')}\n"
        f"placeName: {event.get('placeName')}\n"
    )

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "responseMimeType": "application/json"},
    }

    resp = requests.post(endpoint, params={"key": api_key}, json=payload, timeout=40)
    _log(f"Gemini response HTTP {resp.status_code}")
    if resp.status_code >= 400:
        _log(f"Gemini error body: {resp.text[:300]}")
        return {"category": "unknown", "confidence": 0.0}

    data = resp.json()
    text = ""
    candidates = data.get("candidates", [])
    if candidates:
        parts = candidates[0].get("content", {}).get("parts", [])
        if parts:
            text = parts[0].get("text", "")

    parsed = _extract_json(text)
    category = str(parsed.get("category", "unknown")).lower().strip()
    confidence_raw = parsed.get("confidence", 0.0)
    try:
        confidence = float(confidence_raw)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    if category not in CATEGORIES:
        category = "unknown"
        confidence = 0.0

    _log(f"Classified category='{category}' confidence={confidence}")
    return {"category": category, "confidence": confidence}


def classify_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    _log(f"Classifying {len(events)} events")
    output = []
    for idx, event in enumerate(events, start=1):
        _log(f"Classifying event {idx}/{len(events)}")
        event_copy = copy.deepcopy(event)
        event_copy["aiCategory"] = classify_event(event_copy)
        output.append(event_copy)
    _log("Classification done")
    return output
