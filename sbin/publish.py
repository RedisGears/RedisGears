import os
import argparse
import boto3
from pathlib import Path


def upload_directory_to_s3(local_directory, s3_destination):
    s3_client = boto3.client("s3")
    local_path = Path(local_directory)

    s3_path = s3_destination.replace("s3://", "")
    bucket, prefix = s3_path.split("/", 1)

    if not prefix.endswith("/"):
        prefix += "/"

    for file_path in local_path.iterdir():
        if file_path.is_file():
            s3_key = prefix + file_path.name
            s3_client.upload_file(str(file_path), bucket, s3_key, ExtraArgs={"ACL": "public-read"})
            print(f"Uploaded {file_path.name} to s3://{bucket}/{s3_key}")


def main():
    parser = argparse.ArgumentParser(description="Upload artifacts to S3")
    parser.add_argument(
        "--snapshot", action="store_true", help="Upload snapshot artifacts to s3://redismodules/redisgears/snapshots/"
    )
    parser.add_argument(
        "--release", action="store_true", help="Upload release artifacts to s3://redismodules/redisgears/"
    )

    args = parser.parse_args()

    if args.snapshot:
        print("Uploading snapshot artifacts...")
        upload_directory_to_s3("./artifacts/snapshot/", "s3://redismodules/redisgears/snapshots/")

    if args.release:
        print("Uploading release artifacts...")
        upload_directory_to_s3("./artifacts/release/", "s3://redismodules/redisgears/")


if __name__ == "__main__":
    assert os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    assert os.getenv("AWS_ACCESS_KEY_ID")
    assert os.getenv("AWS_SECRET_ACCESS_KEY")
    main()
