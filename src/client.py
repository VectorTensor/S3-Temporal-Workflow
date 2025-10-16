from temporalio.client import Client
import asyncio


async def main():

    client: Client = await Client.connect("localhost:7233")

    handler = client.get_workflow_handle(
        "56ca28e6-c041-46a3-9807-07d239b8e023")
    await handler.signal("special_data", 5)


asyncio.run(main())
