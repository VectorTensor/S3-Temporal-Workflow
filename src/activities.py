
# activities.py
from temporalio import workflow
with workflow.unsafe.imports_passed_through():
    import boto3
    import pandas as pd
    from io import StringIO
from temporalio import activity
from dataclasses import dataclass
import asyncio

s3_client = boto3.client("s3")


@dataclass
class DownloadCsvRequest:
    bucket_name: str
    key: str


@activity.defn
async def download_csv(request: DownloadCsvRequest) -> str:
    bucket_name = request.bucket_name
    key = request.key
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    data = obj["Body"].read().decode("utf-8")
    return data


@activity.defn
async def transform_csv(csv_content: str) -> str:
    # Example transformation: convert all column names to uppercase
    df = pd.read_csv(StringIO(csv_content))
    df.columns = [col.upper() for col in df.columns]
    # Add any other transformations here
    print(df.columns)
    asyncio.sleep(30)
    df["INC"] = df["SALARY"] + 10

    output = StringIO()
    df.to_csv(output, index=False)
    return output.getvalue()


@activity.defn(name="transform_csv")
async def mock_transform_csv(csv_content: str) -> str:
    return """
        data\n
        1.0\n
        2.0\n

    """


@dataclass
class UploadCsvRequest:
    bucket_name: str
    key: str
    csv_content: str


@activity.defn
async def upload_csv(request: UploadCsvRequest):
    bucket_name = request.bucket_name
    key = request.key
    csv_content = request.csv_content
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=csv_content)
