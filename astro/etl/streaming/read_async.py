import asyncio
import os
from dotenv import load_dotenv
from azure.eventhub.aio import EventHubConsumerClient

from send_to_sqs import MessagerSender
import json
load_dotenv()


CONSUMER_GROUP = os.getenv("CONSUMER_GROUP")
CONNECTION_STR = os.getenv("CONNECTION_STR")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")

messager = MessagerSender()

async def on_event(partition_context, event):
    messager.send_message(json.dumps(event.body_as_json(encoding='UTF-8')))
    await partition_context.update_checkpoint(event)


async def main():
    client = EventHubConsumerClient.from_connection_string(CONNECTION_STR, CONSUMER_GROUP)
    async with client:

        await client.receive(on_event=on_event, starting_position="-1")

    await credential.close()

if __name__ == "__main__":
    asyncio.run(main())