# logger.py
import logging

def get_logger(name: str, log_level: str = "INFO"):
    """
    Returns a configured logger that writes to console.
    Log level can be set via the config or environment.
    """
    logger = logging.getLogger(name)
    log_level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(log_level)

    # Create console handler and set level
    ch = logging.StreamHandler()
    ch.setLevel(log_level)

    # Create formatter and add to console handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    # Add handler to logger
    if not logger.handlers:  # To avoid duplicating handlers
        logger.addHandler(ch)

    return logger# TODO