from src.core.logger import get_logger
import logging

logger = get_logger(level=logging.WARNING, name=__name__)

def testik():
    logger.info('infolevel alpy')
    logger.error('errorlevel alpy')
