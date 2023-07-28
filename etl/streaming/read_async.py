import asyncio
import os
from dotenv import load_dotenv
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import (
    BlobCheckpointStore,
)
from azure.identity.aio import DefaultAzureCredential

load_dotenv()


CONNECTION_STR = os.getenv("CONNECTION_STR")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")

credential = DefaultAzureCredential()

async def on_event(partition_context, event):
    print(
        'Received the event: "{}" from the partition with ID: "{}"'.format(
            event.body_as_str(encoding="UTF-8"), partition_context.partition_id
        )
    )

    await partition_context.update_checkpoint(event)


async def main():
    client = EventHubConsumerClient.from_connection_string(CONNECTION_STR, EVENTHUB_NAME)
    async with client:

        await client.receive(on_event=on_event, starting_position="-1")

    await credential.close()

if __name__ == "__main__":
    asyncio.run(main())