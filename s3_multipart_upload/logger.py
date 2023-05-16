import logging
import sys

class CustomFormatter(logging.Formatter):
  """ Taken from https://stackoverflow.com/questions/384076/how-can-i-color-python-logging-output """
  GREY = "\x1b[38;20m"
  GREEN = "\x1b[32;20m"
  YELLOW = "\x1b[33;20m"
  RED = "\x1b[31;20m"
  BOLD_RED = "\x1b[31;1m"
  RESET = "\x1b[0m"
  FORMAT = f"{{0}}[%(asctime)s - %(name)s %(levelname)s]{RESET} %(message)s (%(filename)s:%(lineno)d)"

  LEVEL_COLORS = {
    logging.DEBUG: GREY,
    logging.INFO: GREEN,
    logging.WARNING: YELLOW,
    logging.ERROR: RED,
    logging.CRITICAL: BOLD_RED,
  }

  def format(self, record):
    log_fmt = self._get_format(record.levelno)
    formatter = logging.Formatter(log_fmt)
    return formatter.format(record)
  
  def _get_format(self, level: int):
    color = self.LEVEL_COLORS[level]
    return self.FORMAT.format(color)

def get_logger(name: str):
  logger = logging.getLogger(name)
  logger.setLevel(logging.DEBUG)

  ch = logging.StreamHandler(stream=sys.stdout)
  ch.setLevel(logging.DEBUG)
  ch.setFormatter(CustomFormatter())

  logger.addHandler(ch)
  return logger
