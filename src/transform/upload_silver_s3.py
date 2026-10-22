import os
import sys

from botocore.exceptions import ClientError

from src.ingest.upload_s3 import upload_to_s3


def upload_directory_to_s3(local_dir: str, bucket: str, s3_prefix: str) -> list[str]:
    uploaded_keys = []

    for root, _dirs, files in os.walk(local_dir):
        for filename in files:
            if filename.startswith(".") and filename.endswith(".crc"):
                continue

            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_dir)
            key = f"{s3_prefix}/{relative_path}"

            upload_to_s3(local_path, bucket, key)
            uploaded_keys.append(key)

    return uploaded_keys


def main() -> int:
    bucket = os.environ["S3_BUCKET"]
    local_dir = "data/silver/"
    s3_prefix = "silver"

    try:
        uploaded_keys = upload_directory_to_s3(local_dir, bucket, s3_prefix)
    except ClientError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    for key in uploaded_keys:
        print(f"Uploaded s3://{bucket}/{key}")
    print(f"Uploaded {len(uploaded_keys)} files to s3://{bucket}/{s3_prefix}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
