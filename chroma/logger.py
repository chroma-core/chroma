import logging


def setup_logging():
    logging.basicConfig(filename="chroma_logs.log")
    logger = logging.getLogger("Chroma")
    logger.setLevel(logging.DEBUG)
    logger.debug("Logger created")
    return logger


logger = setup_logging()
