# Kamgo FB Events Prototype

Simple Python prototype that:
- takes one Facebook page URL
- fetches events via Bright Data dataset API (`discover_by=venue`)
- maps output to simplified Kamgo schema
- estimates source activity
- classifies events with Gemini/Gemma model
- saves JSON outputs

## Files
- `main.py`
- `scrape_events.py`
- `classify_events.py`
- `.env`

## Setup
1. Install dependency:
```bash
pip install requests
```
2. Edit `.env` and fill:
- `BRIGHTDATA_API_TOKEN`
- `BRIGHTDATA_DATASET_ID`
- `GEMINI_API_KEY`

`BRIGHTDATA_DATASET_ID` is the scraper ID in Bright Data (usually starts with `gd_...`).
You can copy it from the browser URL or code example panel in Bright Data.

## Run
```bash
python main.py
```

## Outputs
- `events_raw.json`
- `events_classified.json`

Debug logs are printed to CMD by default for every pipeline step (fetch/map/activity/classify/save).
