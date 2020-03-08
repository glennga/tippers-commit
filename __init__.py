import logging.config
import json

with open('config/logging.json') as logging_config_file:
    logging_json = json.load(logging_config_file)
    logging.config.dictConfig(logging_json)
