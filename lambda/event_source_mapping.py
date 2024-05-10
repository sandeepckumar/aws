import logging
import boto3
import time
from os import environ
import json
from base64 import b64decode
from gzip import decompress

logging.getLogger().setLevel(logging.INFO)


class EventLogger:
    def __init__(self):
        self.cw_logs_client = boto3.client("logs")
        self.log_group_name = environ["CW_LOG_GROUP_NAME"]
        self.log_stream_name = ""
        if self.log_group_name.startswith("arn"):
            self.log_group_name = self.log_group_name.split(":")[-1]

    @staticmethod
    def decompress_data(data):
        try:
            logging.info("Decompressing Record Data")
            decomp_data = b64decode(data)
            decomp_data = decompress(decomp_data)
            return json.loads(decomp_data)
        except Exception as e:
            logging.error("Error occurred decompressing data %s", e)

    def extract_data(self, event):
        records = event["Records"]
        json_data = []
        for record in records:
            data = record["kinesis"]["data"]
            json_data.append(self.decompress_data(data))
        return json_data

    def log_event(self, event):
        try:
            data = self.extract_data(event)
            for log in data:
                logEvents = []
                self.log_stream_name = log["logStream"]
                self.cw_logs_client.create_log_stream(logGroupName=self.log_group_name,
                                                      logStreamName=self.log_stream_name)
                logging.info(f"Created log stream: {self.log_stream_name}")
                for logEvent in log["logEvents"]:
                    logEvents.append({"timestamp": logEvent["timestamp"], "message": logEvent["message"]})
                self.cw_logs_client.put_log_events(logGroupName=self.log_group_name,
                                                   logStreamName=self.log_stream_name,
                                                   logEvents=logEvents)
                logging.info(f"Successfully logged event to {self.log_stream_name}")

        except self.cw_logs_client.exceptions.ResourceAlreadyExistsException:
            logging.info(f"Log stream already exists: {self.log_stream_name}")

        except Exception as e:
            logging.error(f"Failed to log event: {e}")


def lambda_handler(event, context):
    logger = EventLogger()
    logger.log_event(event)
    return "Event Logged successfully"
