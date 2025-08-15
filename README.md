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