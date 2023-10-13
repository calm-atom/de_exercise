import logging
import sys


def setup_logger(name: str, level: str = logging.INFO) -> logging.Logger:
    # Create a logger using the passed in name
    logger = logging.getLogger(name)

    # Setup a stream handler to output to stdout
    handler = logging.StreamHandler(stream=sys.stdout)

    # Format the output produced by the logger
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s',
                                  '%Y-%m-%d %H:%M:%S')

    # Attach the formatter to the handler
    handler.setFormatter(formatter)

    # Attach the handler to the logger
    logger.addHandler(handler)

    # Set log level based on the level passed in
    logger.setLevel(level)

    # Return logger
    return logger
