import asyncio
import logging
from datetime import date
import datetime as dt
import re
import random
from bs4 import BeautifulSoup
from fetching import resilient_get
from sources import (
    SourceAdapter,
    RawRaceDocument,
    RunnerDoc,
    FieldConfidence,
    register_adapter,
)
from normalizer import canonical_track_key, canonical_race_key

@register_adapter
class RacingPostAdapter:
    """
    Adapter for fetching racecards from Racing Post.
    This adapter performs a two-stage fetch:
    1. Fetches the main racecards page to get a list of all races.
    2. Fetches the individual page for each race to get runner details.
    """
    source_id = "racing_post"

    def _find_site_config(self, config: dict) -> dict | None:
        """Finds the specific configuration for Racing Post from the main config."""
        logging.info("Searching for Racing Post configuration...")
        for category in config.get("DATA_SOURCES", []):
            for site in category.get("sites", []):
                site_name = site.get("name", "").lower()
                logging.info(f"Checking site for 'racingpost': {site_name}")
                if "racingpost" in site_name.replace(" ", ""):
                    logging.info(f"Found Racing Post config: {site}")
                    return site
        logging.error("Failed to find any site configuration containing 'racingpost'")
        return None

    def _parse_runner_data(self, race_soup: BeautifulSoup) -> list[RunnerDoc]:
        """Parses the runner data from a single race page."""
        # This is a placeholder and will be replaced with Racing Post specific selectors
        runners = []
        runner_rows = race_soup.select("tbody.rp-horse-row")

        for row in runner_rows:
            try:
                horse_name_el = row.select_one("td.rp-td-horse-name a.rp-horse")
                saddle_cloth_el = row.select_one("td.rp-td-horse-entry span.rp-entry-number")
                jockey_el = row.select_one("td.rp-td-horse-jockey a")
                trainer_el = row.select_one("td.rp-td-horse-trainer a")
                odds_el = row.select_one("td.rp-td-horse-prices a.price")

                if not all([horse_name_el, saddle_cloth_el, jockey_el, trainer_el]):
                    continue

                horse_name = horse_name_el.get_text(strip=True)
                saddle_cloth = saddle_cloth_el.get_text(strip=True)
                jockey_name = jockey_el.get_text(strip=True)
                trainer_name = trainer_el.get_text(strip=True)

                odds = None
                if odds_el and odds_el.has_attr('data-price'):
                    odds_val = odds_el['data-price']
                    odds = FieldConfidence(odds_val, 0.9, "td.rp-td-horse-prices a.price[data-price]")

                runner_id = f"{saddle_cloth}-{horse_name}".lower().replace(" ", "-")

                runners.append(RunnerDoc(
                    runner_id=runner_id,
                    name=FieldConfidence(horse_name, 0.95, "td.rp-td-horse-name a.rp-horse"),
                    number=FieldConfidence(saddle_cloth, 0.95, "td.rp-td-horse-entry span.rp-entry-number"),
                    odds=odds,
                    jockey=FieldConfidence(jockey_name, 0.9, "td.rp-td-horse-jockey a"),
                    trainer=FieldConfidence(trainer_name, 0.9, "td.rp-td-horse-trainer a")
                ))
            except Exception as e:
                logging.error(f"Failed to parse a runner row on Racing Post: {e}", exc_info=True)
        return runners

    async def fetch(self, config: dict) -> list[RawRaceDocument]:
        """
        Fetches the Racing Post racecards page, then fetches each individual
        race page to extract detailed runner information.
        """
        site_config = self._find_site_config(config)
        if not site_config:
            logging.error("Racing Post site configuration not found.")
            return []

        base_url = site_config.get("base_url")
        target_url = site_config.get("url")

        if not base_url or not target_url:
            logging.error("Racing Post base_url or url not configured.")
            return []

        # 1. Fetch the main race list page
        logging.info("Fetching Racing Post race list...")
        try:
            list_response = await resilient_get(target_url, config=config)
            list_html = list_response.text
            # Save for debugging
            with open("debug_racingpost_list.html", "w", encoding="utf-8") as f:
                f.write(list_html)
            logging.info("Saved Racing Post race list page to debug_racingpost_list.html")
        except Exception as e:
            logging.error(f"An error occurred while fetching Racing Post race list: {e}")
            return []

        # For now, we just want to inspect the file. Return empty list.
        logging.info("Finished debug fetch for Racing Post. Returning empty list.")
        return []
