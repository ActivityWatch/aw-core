import json
import os
import logging
from typing import Mapping, List, Union, Sequence

import appdirs

from aw_core.models import Event

try:
    import pymongo
except ImportError:
    logging.warning("Could not import pymongo, not available as a datastore backend")


class StorageStrategy():
    """
    Interface for storage methods.

    Implementations require:
     - insert_one
     - get

    Optional:
     - insert_many
    """

    def create_bucket(self):
        raise NotImplementedError

    def get_bucket(self, bucket: str):
        return self.metadata(bucket)

    # Deprecated, use self.get_bucket instead
    def metadata(self, bucket: str):
        raise NotImplementedError

    def get_events(self, bucket: str, limit: int):
        return self.get(bucket, limit)

    # Deprecated, use self.get_events instead
    def get(self, bucket: str, limit: int):
        raise NotImplementedError

    # TODO: Rename to insert_event, or create self.event.insert somehow
    def insert(self, bucket: str, events: Union[Event, Sequence[Event]]):
        #if not (isinstance(events, Event) or isinstance(events, Sequence[Event])) \
        #    and isinstance(events, dict) or isinstance(events, Sequence[dict]):
        #    logging.warning("Events are of type dict, please turn them into proper Events")

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


class MongoDBStorageStrategy(StorageStrategy):
    """Uses a MongoDB server as backend"""

    def __init__(self):
        self.logger = logging.getLogger("datastore-mongodb")

        if 'pymongo' not in vars() and 'pymongo' not in globals():
            self.logger.error("Cannot use the MongoDB backend without pymongo installed")
            exit(1)

        try:
            self.client = pymongo.MongoClient(serverSelectionTimeoutMS=5000)
            self.client.server_info() # Try to connect to the server to make sure that it's available
        except pymongo.errors.ServerSelectionTimeoutError:
            self.logger.error("Couldn't connect to MongoDB server at localhost")
            exit(1)

        # TODO: Readd testing ability
        # self.db = self.client["activitywatch" if not testing else "activitywatch_testing"]
        self.db = self.client["activitywatch"]

    def buckets(self):
        return [{"id": bucket_id} for bucket_id in self.db.collection_names()]

    def get(self, bucket: str, limit: int):
        return list(self.db[bucket].find().sort([("timestamp", -1)]).limit(limit))

    def insert_one(self, bucket: str, event: Event):
        self.db[bucket].insert_one(event)


class MemoryStorageStrategy(StorageStrategy):
    """For storage of data in-memory, useful primarily in testing"""

    def __init__(self):
        self.logger = logging.getLogger("datastore-memory")
        # self.logger.warning("Using in-memory storage, any events stored will not be persistent and will be lost when server is shut down. Use the --storage parameter to set a different storage method.")
        self.db = {}  # type: Mapping[str, Mapping[str, List[Event]]]

    def buckets(self):
        return [{"id": bucket_id} for bucket_id in self.db]

    def get(self, bucket: str, limit: int):
        if bucket not in self.db:
            return []
        return self.db[bucket][-limit:]

    def insert_one(self, bucket: str, event: Event):
        if bucket not in self.db:
            self.db[bucket] = []
        self.db[bucket].append(event)


class FileStorageStrategy(StorageStrategy):
    """For storage of data in JSON files, useful as a zero-dependency/databaseless solution"""

    def __init__(self, maxfilesize=10**5):
        self.logger = logging.getLogger("datastore-files")
        self._fileno = 0
        self._maxfilesize = maxfilesize

    @staticmethod
    def _get_bucketsdir():
        # TODO: Separate testing buckets from production depending on if --testing is set
        testing = False

        user_data_dir = appdirs.user_data_dir("aw-server", "activitywatch")
        buckets_dir = user_data_dir + ("/testing" if testing else "") + "/buckets"
        if not os.path.exists(buckets_dir):
            os.makedirs(buckets_dir)
        return buckets_dir

    def _get_filename(self, bucket: str, fileno: int = None):
        if fileno is None:
            fileno = self._fileno

        bucket_dir = self._get_bucketsdir() + "/" + bucket
        if not os.path.exists(bucket_dir):
            os.makedirs(bucket_dir)

        return "{bucket_dir}/events-{fileno}.json".format(bucket_dir=bucket_dir, fileno=self._fileno)

    def _read_file(self, bucket, fileno):
        filename = self._get_filename(bucket, fileno=fileno)
        if not os.path.isfile(filename):
            return []
        with open(filename, 'r') as f:
            data = json.load(f)
        return data

    def get(self, bucket: str, limit: int):
        filename = self._get_filename(bucket)
        if not os.path.isfile(filename):
            return []
        with open(filename) as f:
            # FIXME: I'm slow and memory consuming with large files, see this:
            # https://stackoverflow.com/questions/2301789/read-a-file-in-reverse-order-using-python
            data = [json.loads(line) for line in f.readlines()[-limit:]]
        return data

    def create_bucket(self):
        raise NotImplementedError

    def buckets(self):
        return [self.metadata(bucket_id) for bucket_id in os.listdir(self._get_bucketsdir())]

    def metadata(self, bucket: str):
        # TODO: Implement properly (store metadata in <bucket>/metadata.json)
        return {
            "id": bucket,
            "hostname": "unknown",
            "client": "unknown"
        }

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
