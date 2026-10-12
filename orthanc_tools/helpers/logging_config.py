import logging
import os


def configure_logging(level=logging.INFO):
    """Configure CLI logs without flooding application output with HTTP retries."""
    urllib3_level = os.environ.get("URLLIB3_LOG_LEVEL", "ERROR").upper()
    levels = logging.getLevelNamesMapping()
    if urllib3_level not in levels:
        raise ValueError(f"Invalid URLLIB3_LOG_LEVEL: {urllib3_level}")

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    logging.getLogger("urllib3.connectionpool").setLevel(levels[urllib3_level])
