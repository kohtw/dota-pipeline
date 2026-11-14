"""Garage (S3-compatible) raw bucket. Config from env: S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY."""

import io
import os
from functools import cache

import boto3
import requests
import zstandard
from botocore.config import Config
from botocore.exceptions import ClientError

from dota.config import BUCKET
from dota.errors import PermanentError, is_transient_status, redact

DEM_MAGIC = b"PBDEMS2\0"  # first bytes of every Source 2 replay


# one client per process: building one loads boto3's service model each time, and a task like
# check_contract calls this per object. boto3 clients are thread-safe, and the env never changes.
@cache
def client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",  # must match s3_region in garage.toml
        config=Config(s3={"addressing_style": "path"}),
    )


def exists(key: str) -> bool:
    try:
        client().head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def read(key: str) -> bytes:
    return client().get_object(Bucket=BUCKET, Key=key)["Body"].read()


def put(key: str, body: bytes = b"") -> None:
    client().put_object(Bucket=BUCKET, Key=key, Body=body)


def delete_key(key: str) -> None:
    client().delete_object(Bucket=BUCKET, Key=key)


def list_keys(prefix: str) -> list[str]:
    keys = []
    for page in client().get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys += [obj["Key"] for obj in page.get("Contents", [])]
    return keys


def upload_replay(url: str, key: str) -> None:
    """Stream a Valve replay into the bucket, decompressing on the way (no local file).
    Valve serves zstd despite the .bz2 name. A multipart upload only appears when
    complete, so a failed run never leaves a partial object."""
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            if not r.ok:
                problem = f"replay download for {key} failed: HTTP {r.status_code}"
                raise RuntimeError(problem) if is_transient_status(r.status_code) else PermanentError(problem)
            with zstandard.ZstdDecompressor().stream_reader(r.raw) as dem:
                # a body that is not a replay would otherwise land in Garage and crash the parser
                # in every sweep until quarantined; peek leaves the header in the upload
                replay = io.BufferedReader(dem)
                try:
                    head = replay.peek(len(DEM_MAGIC))[:len(DEM_MAGIC)]
                except zstandard.ZstdError:  # not zstd at all, e.g. an HTML error page
                    head = b""
                if head != DEM_MAGIC:
                    raise PermanentError(f"replay download for {key} is not a Dota 2 replay")
                client().upload_fileobj(replay, BUCKET, key)
    except requests.RequestException as e:
        raise RuntimeError(redact(f"replay download for {key} failed: {e}")) from None
