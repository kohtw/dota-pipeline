"""In-memory stand-ins for an S3 client and the clocks, shared by the cold store tests."""

import io

from botocore.exceptions import ClientError


class FakeS3:
    """Enough of a boto3 S3 client. upload_fileobj reads in parts like a multipart upload and
    stores nothing when a read raises; tests that need s3transfer's own behaviour use a real
    client instead."""

    def __init__(self, part=64 * 1024):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.part = part
        self.puts: list[str] = []

    def _missing(self, op):
        return ClientError({"Error": {"Code": "404" if op == "HeadObject" else "NoSuchKey"}}, op)

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise self._missing("GetObject")
        data = self.objects[(Bucket, Key)]
        return {"Body": io.BytesIO(data), "ContentLength": len(data)}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise self._missing("HeadObject")
        return {}

    def put_object(self, Bucket, Key, Body=b""):
        self.objects[(Bucket, Key)] = Body.read() if hasattr(Body, "read") else Body
        self.puts.append(Key)

    def upload_fileobj(self, f, bucket, key):
        parts = []
        while chunk := f.read(self.part):
            parts.append(chunk)
        self.objects[(bucket, key)] = b"".join(parts)

    def delete_object(self, Bucket, Key):
        self.objects.pop((Bucket, Key), None)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        objects = self.objects

        class Paginator:
            def paginate(self, Bucket, Prefix):
                keys = sorted(k for b, k in objects if b == Bucket and k.startswith(Prefix))
                yield {"Contents": [{"Key": k} for k in keys]} if keys else {}

        return Paginator()


class Clock:
    """Wall and monotonic clocks that advance together on sleep. jump() steps only the wall clock,
    as NTP or a VM resync would."""

    def __init__(self, t=1_800_000_000.0):
        self.t, self.m, self.sleeps = t, 0.0, []

    def now(self):
        return self.t

    def mono(self):
        return self.m

    def sleep(self, s):
        self.sleeps.append(s)
        self.advance(s)

    def advance(self, s):
        self.t += s
        self.m += s

    def jump(self, s):
        self.t += s
