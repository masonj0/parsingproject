# run_pipeline_test.py
import asyncio
import logging
from config import load_config
from main import run_adapter_pipeline
import enhanced_scanner # This is important to ensure adapters are registered
from fetching import close_shared_async_client

async def main():
    """
    Runs the full V2 adapter pipeline for end-to-end testing.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    config = load_config()
    if not config:
        logging.critical("Failed to load configuration. Exiting.")
        return

    try:
        await run_adapter_pipeline(config)
    finally:
        # Ensure the shared client is closed even if the pipeline fails
        await close_shared_async_client()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"An unexpected error occurred during the pipeline test run: {e}", exc_info=True)
