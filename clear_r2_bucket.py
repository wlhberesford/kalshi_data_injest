"""
clear_r2_bucket.py
Deletes every object in the R2 bucket. Run once to start fresh.
"""

import os
import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["R2_ENDPOINT_URL"],
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    config=Config(signature_version="s3v4"),
    region_name="auto",
)

BUCKET = os.environ["R2_BUCKET_NAME"]

paginator = s3.get_paginator("list_objects_v2")
deleted = 0

for page in paginator.paginate(Bucket=BUCKET):
    objects = page.get("Contents", [])
    if not objects:
        continue
    keys = [{"Key": o["Key"]} for o in objects]
    s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys})
    deleted += len(keys)
    print(f"  Deleted {deleted} objects so far...")

print(f"✅ Done — {deleted} objects removed from {BUCKET}")
