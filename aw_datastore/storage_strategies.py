import json
import os
import logging
import sys
from typing import List, Union, Sequence

import appdirs
from shutil import rmtree

from aw_core.models import Event

try:
    import pymongo
except ImportError:
    logging.warning("Could not import pymongo, not available as a datastore backend")

logger = logging.getLogger("aw.datastore.strategies")


class StorageStrategy():
    """
    Interface for storage methods.

    Implementations require:
     - insert_one
     - get

    Optional:
     - insert_many
    """

    def __init__(self, testing):
        self.testing = testing

    def create_bucket(self, bucket_id, type_id, client, hostname, created, name=None):
        raise NotImplementedError

    def delete_bucket(self, bucket_id):
        raise NotImplementedError

    def get_metadata(self, bucket: str):
        raise NotImplementedError

    def buckets(self):
        raise NotImplementedError

    def get_events(self, bucket: str, limit: int):
        return self.get(bucket, limit)

    # Deprecated, use self.get_events instead
    def get(self, bucket: str, limit: int):
        raise NotImplementedError

    # TODO: Rename to insert_event, or create self.event.insert somehow
    def insert(self, bucket: str, events: Union[Event, Sequence[Event]]):
        if isinstance(events, Event) or isinstance(events, dict):
            self.insert_one(bucket, events)
        elif isinstance(events, Sequence):
            self.insert_many(bucket, events)
        else:
            print("Argument events wasn't a valid type")

    def insert_one(self, bucket: str, event: Event):
        raise NotImplementedError

    def insert_many(self, bucket: str, events: List[Event]):
        for event in events:
            self.insert_one(bucket, event)

    def replace_last(self, bucket_id, event):
        raise NotImplementedError


class MongoDBStorageStrategy(StorageStrategy):
    """Uses a MongoDB server as backend"""

    def __init__(self, testing):
        self.logger = logger.getChild("mongodb")

        if 'pymongo' not in vars() and 'pymongo' not in globals():
            self.logger.error("Cannot use the MongoDB backend without pymongo installed")
            exit(1)

        try:
            self.client = pymongo.MongoClient(serverSelectionTimeoutMS=5000)
            # Try to connect to the server to make sure that it's available
            self.client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError:
            self.logger.error("Couldn't connect to MongoDB server at localhost")
            exit(1)

        self.db = self.client["activitywatch" if not testing else "activitywatch-testing"]

    def create_bucket(self, bucket_id, type_id, client, hostname, created, name=None):
        if not name:
            name = bucket_id
        metadata = {
            "_id": "metadata",
            "id": bucket_id,
            "name": name,
            "type": type_id,
            "client": client,
            "hostname": hostname,
            "created": created,
        }
        self.db[bucket_id]["metadata"].insert_one(metadata)

    def delete_bucket(self, bucket_id):
        self.db[bucket_id]["events"].drop()
        self.db[bucket_id]["metadata"].drop()

    def buckets(self):
        bucketnames = set()
        for bucket_coll in self.db.collection_names():
            bucketnames.add(bucket_coll.split('.')[0])
        buckets = {}
        for bucket_id in bucketnames:
            buckets[bucket_id] = self.get_metadata(bucket_id)
        return buckets

    def get_metadata(self, bucket_id: str):
        metadata = self.db[bucket_id]["metadata"].find_one({"_id": "metadata"})
        if metadata:
            del metadata["_id"]
        return metadata

    def get(self, bucket_id: str, limit: int):
        if limit == -1:
            limit = sys.maxsize
        return list(self.db[bucket_id]["events"].find().sort([("timestamp", -1)]).limit(limit))

    def insert_one(self, bucket: str, event: Event):
        self.db[bucket]["events"].insert_one(event)

    def replace_last(self, bucket_id, event):
        last_event = list(self.db[bucket_id]["events"].find().sort([("timestamp", -1)]).limit(1))[0]
        print(last_event)
        self.db[bucket_id]["events"].replace_one({"_id": last_event["_id"]}, event.to_json_dict())



class MemoryStorageStrategy(StorageStrategy):
    """For storage of data in-memory, useful primarily in testing"""

    def __init__(self, testing):
        self.logger = logger.getChild("memory")
        # self.logger.warning("Using in-memory storage, any events stored will not be persistent and will be lost when server is shut down. Use the --storage parameter to set a different storage method.")
        self.db = {}  # type: Mapping[str, Mapping[str, List[Event]]]
        self._metadata = {}

    def create_bucket(self, bucket_id, type_id, client, hostname, created, name=None):
        if not name:
            name = bucket_id
        self._metadata[bucket_id] = {
            "id": bucket_id,
            "name": name,
            "type": type_id,
            "client": client,
            "hostname": hostname,
            "created": created
        }
        self.db[bucket_id] = []

    def delete_bucket(self, bucket_id):
        del self.db[bucket_id]
        del self._metadata[bucket_id]

    def buckets(self):
        buckets = {}
        for bucket_id in self.db:
            buckets[bucket_id] = self.get_metadata(bucket_id)
        return buckets

    def get(self, bucket: str, limit: int):
        if limit == -1:
            limit = sys.maxsize
        if bucket not in self.db:
            return []
        return self.db[bucket][-limit:]

    def get_metadata(self, bucket_id: str):
        return self._metadata[bucket_id]

    def insert_one(self, bucket: str, event: Event):
        if bucket not in self.db:
            self.db[bucket] = []
        self.db[bucket].append(event)

    def replace_last(self, bucket_id, event):
        self.db[bucket_id][-1] = event


class FileStorageStrategy(StorageStrategy):
    """For storage of data in JSON files, useful as a zero-dependency/databaseless solution"""

    def __init__(self, testing, maxfilesize=10**5):
        self.logger = logger.getChild("file")
        self._fileno = 0
        self._maxfilesize = maxfilesize

        # Create dirs
        self.user_data_dir = appdirs.user_data_dir("aw-server", "activitywatch")
        self.buckets_dir = os.path.join(self.user_data_dir, "testing" if testing else "", "buckets")
        if not os.path.exists(self.buckets_dir):
            os.makedirs(self.buckets_dir)

    def _get_bucket_dir(self, bucket_id):
        return os.path.join(self.buckets_dir, bucket_id)

    def create_bucket(self, bucket_id, type_id, client, hostname, created, name=None):
        bucket_dir = self._get_bucket_dir(bucket_id)
        if not os.path.exists(bucket_dir):
            os.makedirs(bucket_dir)
        if not name:
            name = bucket_id
        metadata = {
            "id": bucket_id,
            "name": name,
            "type": type_id,
            "client": client,
            "hostname": hostname,
            "created": created
        }
        with open(os.path.join(bucket_dir, "metadata.json"), "w") as f:
            f.write(json.dumps(metadata))

    def delete_bucket(self, bucket_id):
        rmtree(self._get_bucket_dir(bucket_id))

    def _get_filename(self, bucket_id: str, fileno: int = None):
        bucket_dir = self._get_bucket_dir(bucket_id)
        return os.path.join(bucket_dir, str(self._fileno))

    def _read_file(self, bucket, fileno):
        filename = self._get_filename(bucket, fileno=fileno)
        if not os.path.isfile(filename):
            return []
        with open(filename, 'r') as f:
            data = json.load(f)
        return data

    def get(self, bucket: str, limit: int):
        if limit == -1:
            limit = sys.maxsize
        filename = self._get_filename(bucket)
        if not os.path.isfile(filename):
            return []
        with open(filename) as f:
            # FIXME: I'm slow and memory consuming with large files, see this:
            # https://stackoverflow.com/questions/2301789/read-a-file-in-reverse-order-using-python
            data = [json.loads(line) for line in f.readlines()[-limit:]]
        return data

    def buckets(self):
        buckets = {}
        for bucket_id in os.listdir(self.buckets_dir):
            buckets[bucket_id] = self.get_metadata(bucket_id)
        return buckets

    def get_metadata(self, bucket_id: str):
        metafile = os.path.join(self._get_bucket_dir(bucket_id), "metadata.json")
        with open(metafile, 'r') as f:
            metadata = json.load(f)
        return metadata

    def insert_one(self, bucket: str, event: Event):
        self.insert_many(bucket, [event])

    def insert_many(self, bucket: str, events: Sequence[Event]):
        filename = self._get_filename(bucket)

        # Decide wether to append or create a new file
        """
        if os.path.isfile(filename):
            size = os.path.getsize(filename)
            if size > self._maxfilesize:
                print("Bucket larger than allowed")
                print(size, self._maxfilesize)
        """

        # Option: Limit on events per file instead of filesize
        """
        num_lines = sum(1 for line in open(filename))
        """

        str_to_append = "\n".join([json.dumps(event.to_json_dict()) for event in events])
        with open(filename, "a+") as f:
            f.write(str_to_append + "\n")
    
    def replace_last(self, bucket, event):
        events = self.get(bucket, -1)
        filename = self._get_filename(bucket)
        with open(filename, "w") as f:
            events[0] = event
            newfiledata = "\n".join([json.dumps(event.to_json_dict()) for event in events])
            f.write(newfiledata)
