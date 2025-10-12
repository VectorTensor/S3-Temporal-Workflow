# workflow.py
from temporalio import workflow
from dataclasses import dataclass

with workflow.unsafe.imports_passed_through():
    from datetime import timedelta
    import activities
    from activities import DownloadCsvRequest


@dataclass
class WorkflowRequest:
    source_bucket: str
    source_key: str
    dest_bucket: str
    dest_key: str


@workflow.defn
class S3CsvWorkflow:
    @workflow.run
    async def run(self, request: WorkflowRequest):
        # Step 1: Download CSV from S3

        source_bucket = request.source_bucket
        source_key = request.source_key
        dest_bucket = request.dest_bucket
        dest_key = request.dest_key

        download_csv_params = DownloadCsvRequest(
            bucket_name=source_bucket,
            key=source_key)
        csv_content = await workflow.execute_activity(
            activities.download_csv,
            download_csv_params,
            start_to_close_timeout=timedelta(seconds=30)
        )

        # Step 2: Transform CSV
        transformed_csv = await workflow.execute_activity(
            activities.transform_csv,
            csv_content,
            start_to_close_timeout=timedelta(seconds=30)
        )

        params = activities.UploadCsvRequest(
            bucket_name=dest_bucket,
            key=dest_key,
            csv_content=transformed_csv
        )
        await workflow.execute_activity(
            activities.upload_csv,
            params,
            start_to_close_timeout=timedelta(seconds=30)
        )

        return f"CSV transformed and uploaded to {dest_bucket}/{dest_key}"
