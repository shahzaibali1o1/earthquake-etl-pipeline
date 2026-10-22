import os
import sys

import boto3
from botocore.exceptions import ClientError


def upload_to_s3(file_path: str, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(file_path, bucket, key)


def main() -> int:
    bucket = os.environ["S3_BUCKET"]
    file_path = "data/bronze/raw_2026-01-01.json"
    key = "bronze/2026-01-01/raw_2026-01-01.json"

    try:
        upload_to_s3(file_path, bucket, key)
    except ClientError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    print(f"Uploaded {file_path} to s3://{bucket}/{key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
