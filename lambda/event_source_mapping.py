import logging
import boto3
import time
from os import environ
import json

logging.getLogger().setLevel(logging.INFO)


class EventLogger:
    def __init__(self):
        self.cw_logs_client = boto3.client("logs")
        self.log_group_name = environ["CW_LOG_GROUP_NAME"]

    def log_event(self, event):
        log_stream_name = f"{time.strftime('%Y-%m-%d')}"
        event_log = [{"timestamp": int(event["timestamp"]), "message": json.dumps(event)}]
        print(event_log)
        try:
            self.cw_logs_client.create_log_stream(logGroupName=self.log_group_name,
                                                  logStreamName=log_stream_name)
            logging.info(f"Created log stream: {log_stream_name}")
        except self.cw_logs_client.exceptions.ResourceAlreadyExistsException:
            logging.info(f"Log stream already exists: {log_stream_name}")
        try:

            self.cw_logs_client.put_log_events(logGroupName=self.log_group_name,
                                               logStreamName=log_stream_name,
                                               logEvents=event_log)
            logging.info(f"Successfully logged event to {log_stream_name}")
        except Exception as e:
            logging.error(f"Failed to log event: {e}")


def lambda_handler(event, context):
    logger = EventLogger()
    logger.log_event(event)
    return "Event Logged successfully"
