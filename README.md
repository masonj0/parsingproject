# Paddock Parser Toolkit v1.2

Paddock Parser is a powerful, two-tool toolkit for identifying structurally advantageous betting opportunities in global racing. It is the culmination of a collaborative design process, architected for reliability, intelligence, and a superior user experience.

## The Two-Tool Philosophy

The toolkit consists of two distinct, complementary applications, run from a single entry point:

1.  **The "Quick Strike" Scout (`python main.py scan`):** A fast, fully automated scraper designed to provide a rapid, "good enough" snapshot of the day's racing. It is hardened for corporate networks and uses an intelligent API-first, scrape-as-fallback strategy. Its `--json-out` feature creates a direct data bridge to the Deep Dive Engine.

2.  **The "Deep Dive" Engine (`python main.py persistent`):** A 100% reliable, manual-first, "always-on" analytical engine. It uses a persistent, crash-safe in-memory cache that becomes more intelligent with each block of data you paste from the interactive `collector.html` dashboard. This is the definitive source of truth for deep analysis.

## Core Features

-   **Persistent, Always-On Engine:** Launch the parser once and feed it data all day.
-   **Crash-Safe Daily Cache:** Your session data is automatically backed up to a JSON file and can be restored on startup.
-   **Intelligent Data Merging:** The cache gets smarter with each paste, preferring known odds over SP and enriching race data over time.
-   **Clipboard-First Workflow:** An innovative `collector.html` dashboard and a sentinel-based paste listener create a frictionless data collection experience.
-   **Dual-Proxy System:** The collector includes special proxy links to help bypass corporate firewalls, making every data source "work-friendly."
-   **Powerful CLI & Interactive Menu:** Run via a friendly menu (by double-clicking) or use the powerful command-line interface for automation and advanced filtering.
-   **Shared Intelligence:** A common `normalizer.py` module ensures 100% data consistency across both tools.

## Installation

1.  **Prerequisites:** Ensure you have Python 3.8 or newer installed.
2.  **Create a Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    # On Windows:
    # venv\Scripts\activate
    # On macOS/Linux:
    # source venv/bin/activate
    ```
3.  **Install Dependencies:** With your virtual environment active, run:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

**Interactive Menu (Easiest Method):**
Simply run the main script with no arguments.
```bash
python main.py```

**Command-Line Interface (for Power Users & Automation):**
```bash
# Run the automated "Quick Strike" scan
python main.py scan

# Launch the "Deep Dive" persistent engine
python main.py persistent

# Just open the data collection helper page
python main.py collect