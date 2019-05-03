from flask import current_app
from google.auth import credentials, default as default_creds
from google.cloud import bigtable, datastore

import sys
import numpy as np
import logging
import time

from neuronmeshdb import meshdb

cache = {}

from pythonjsonlogger import jsonlogger


class JsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        """Remap `log_record`s fields to fluentd-gcp counterparts."""
        super(JsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record["time"] = log_record.get("time", log_record["asctime"])
        log_record["severity"] = log_record.get(
            "severity", log_record["levelname"])
        log_record["source"] = log_record.get("source", log_record["name"])
        del log_record["asctime"]
        del log_record["levelname"]
        del log_record["name"]



class DoNothingCreds(credentials.Credentials):
    def refresh(self, request):
        pass


def get_bigtable_client(config):
    project_id = config.get('project_id', 'pychunkedgraph')

    if config.get('emulate', False):
        credentials = DoNothingCreds()
    else:
        credentials, project_id = default_creds()

    client = bigtable.Client(admin=True,
                             project=project_id,
                             credentials=credentials)
    return client


def get_cg(table_id):
    if table_id not in cache:
        instance_id = current_app.config['CHUNKGRAPH_INSTANCE_ID']
        client = get_bigtable_client(current_app.config)

        # Create ChunkedGraph logging
        logger = logging.getLogger(f"{instance_id}/{table_id}")
        logger.setLevel(current_app.config['LOGGING_LEVEL'])

        # prevent duplicate logs from Flasks(?) parent logger
        logger.propagate = False

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(current_app.config['LOGGING_LEVEL'])
        formatter = JsonFormatter(fmt=current_app.config['LOGGING_FORMAT'],
                                  datefmt=current_app.config['LOGGING_DATEFORMAT'])
        formatter.converter = time.gmtime
        handler.setFormatter(formatter)

        logger.addHandler(handler)

        # Create ChunkedGraph
        cache[table_id] = meshdb.MeshDB(table_id=table_id,
                                        instance_id=instance_id,
                                        client=client,
                                        logger=logger)
    current_app.table_id = table_id
    return cache[table_id]