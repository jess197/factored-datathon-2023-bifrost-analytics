import os
import asyncio
import logging
from azure.eventhub import EventHubConsumerClient

import os
from dotenv import load_dotenv

CONSUMER_GROUP = os.getenv("CONSUMER_GROUP")
CONNECTION_STR = os.getenv("CONNECTION_STR")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")

client = EventHubConsumerClient.from_connection_string(CONNECTION_STR, CONSUMER_GROUP)

logger = logging.getLogger("azure.eventhub")
logging.basicConfig(level=logging.INFO)

def on_event(partition_context, event):
    logger.info("Received event from partition {}".format(partition_context.partition_id))
    #partition_context.update_checkpoint(event)
    logger.info(event)

with client:
    client.receive(
        on_event=on_event,
        starting_position="-1",  # "-1" is from the beginning of the partition.
    )
    # receive events from specified partition:
    # client.receive(on_event=on_event, partition_id='0')