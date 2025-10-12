
# worker.py
from temporalio.client import Client
from temporalio.worker import Worker
from transformation_workflow import S3CsvWorkflow
import activities
import asyncio


async def main():
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue="s3-csv-task-queue",
        workflows=[S3CsvWorkflow],
        activities=[activities.download_csv,
                    activities.mock_transform_csv, activities.upload_csv],
    )

    await worker.run()

# Run this with: `python -m asyncio worker.py`
#
asyncio.run(main())
