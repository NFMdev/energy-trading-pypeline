import logging

DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def configure_logging(log_level: str = "INFO") -> None:
    normalized_level = log_level.upper()

    logging.basicConfig(level=normalized_level, format=DEFAULT_LOG_FORMAT)
