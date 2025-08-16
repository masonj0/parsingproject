# Paddock Parser Toolkit v1.6

This project contains two distinct applications built to work together: a comprehensive Desktop Analysis Toolkit and a lightweight Mobile Alerting Engine.

## 1. Desktop Analysis Toolkit

A full-featured suite for deep, interactive analysis of global racing data, designed to be run from a desktop computer.

### Core Features

-   **Automated Pre-Fetch:** A primary menu option that uses a browser disguise to automatically download dozens of accessible racing data sources into the `html_input` folder, automating the most tedious collection work.
-   **Manual Collection & Proxy System:** An innovative `collector.html` dashboard helps you manually gather data from "hard target" sites and includes special proxy links to help bypass corporate firewalls.
-   **Persistent "Live Paste" Engine:** Launch the parser once and feed it data all day via the clipboard.
-   **Crash-Safe Daily Cache:** Your live session data is automatically backed up to a JSON file and can be restored on startup, preventing data loss.
-   **Intelligent Data Merging:** The cache gets smarter with each paste, preferring known odds over SP and enriching race data over time.
-   **Powerful Interactive Menu:** The entire toolkit is run from a simple, user-friendly menu that appears when you run `main.py`.
-   **Shared Intelligence:** A common `normalizer.py` and `analysis.py` module ensures 100% data and scoring consistency across the entire project.

### Usage

1.  Ensure all dependencies are installed: `pip install -r requirements.txt`
2.  Run the main interactive menu: `python main.py`

## 2. Mobile Alerting Engine

A standalone, autonomous intelligence agent designed to run 24/7 on a mobile device (e.g., via Termux on Android). It constantly hunts for high-value opportunities and alerts you via native device notifications.

### Core Features

-   **Continuous Monitoring:** Runs in an infinite loop, periodically scanning a curated list of reliable "soft target" data sources.
-   **Proactive Alerting:** Uses the same powerful `EnhancedValueScorer` as the desktop toolkit. When a race meets the `MINIMUM_SCORE_TO_ALERT` defined in `mobile_config.json`, it sends a native notification to your device.
-   **Stateful Awareness:** Remembers which opportunities it has already alerted on for the day by using a `daily_alerts.json` state file, preventing duplicate notifications.

### Usage

1.  Transfer `mobile_alert_engine.py`, `mobile_config.json`, `analysis.py`, and `normalizer.py` to your device.
2.  Install the Termux:API app from the app store to enable notifications.
3.  Run the engine from the Termux command line: `python mobile_alert_engine.py`

## 3. Developer & Advanced Usage

### How to Add a New Source Adapter (v2 Architecture)

The new adapter-based architecture makes adding new data sources straightforward and safe. Follow these steps:

1.  **Create an Adapter Class:** In `enhanced_scanner.py` (or a new file in a future `adapters/` directory), create a new class (e.g., `MyNewSourceAdapter`).
2.  **Implement the `SourceAdapter` Protocol:** Ensure your class implements the `SourceAdapter` protocol from `sources.py`.
3.  **Set `source_id`:** Assign a unique, lowercase `source_id` string to your class. This is used for logging and tracking.
4.  **Implement `async def fetch(self, config: dict)`:** This is the main method. It should:
    *   Use the functions in `fetching.py` (e.g., `breadcrumb_get`, `resilient_get`) to get the raw HTML/JSON data.
    *   If successful, save the raw content to a file in the `html_input` directory.
    *   Parse the raw content using a library like BeautifulSoup.
    *   Transform the parsed data into a list of `RawRaceDocument` objects.
    *   Populate the `FieldConfidence` objects for each field to track data quality.
5.  **Register Your Adapter:** Add the `@register_adapter` decorator to your class.
6.  **Add Configuration:** Add a corresponding entry for your new source in `config.json` under `DATA_SOURCES`, ensuring the `name` matches what your adapter expects.

That's it! The `main.py` pipeline will automatically discover and run your new adapter.

### Scoring Explanation (v2 Engine)

The new analysis engine in `analysis.py` uses a "signal-based" approach for scoring races. This is a more transparent and powerful method than the previous scorer.

-   **Signals:** Instead of a single monolithic score, the system first computes several individual "signals" for each race (e.g., `overlay_confidence`, `market_consensus`, `steam_move`). These represent different aspects of a race's value.
-   **Weights:** These signals are combined into a final score using a set of weights defined in `DEFAULT_WEIGHTS` in `analysis.py`.
-   **Track Personas:** The system can apply "track profiles" (from `TRACK_PROFILES`) to adjust signal weights for specific tracks. For example, a "steam move" might be more significant at Ascot than at other tracks.
-   **Explainability:** The final `ScoreResult` object includes not just the total score, but also the raw signal values and a list of "reasons" explaining how the score was calculated.

### Mobile Tuning (v2 Engine)

The mobile alerting engine is designed to be highly tunable to match your risk tolerance. The key settings will be moved to `mobile_config.json`:

-   **`COOLDOWN_MINUTES`**: Sets the minimum time (in minutes) before the engine will send another alert for the same race, preventing spam.
-   **`SIGNAL_THRESH`**: Defines the minimum value a specific signal must have to be considered "active."
-   **`MIN_SIGNALS_OVER_THRESH`**: The core of the composite alerting rule. This sets how many different signals must be over their threshold at the same time to trigger an alert. For example, a value of `2` means a race might need both a high `overlay_confidence` AND a significant `steam_move` to be considered interesting.