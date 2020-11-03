from typing import Optional, List, Dict, Any
from datetime import datetime, timezone,time
import json
import os
import logging
import iso8601
import re

from peewee import (
    Model,
    CharField,
    IntegerField,
    DecimalField,
    DateTimeField,
    DateField,
    ForeignKeyField,
    AutoField
)
from peewee import fn
from playhouse.sqlite_ext import SqliteExtDatabase

from aw_core.models import Event
from aw_core.dirs import get_data_dir

from .abstract import AbstractStorage

logger = logging.getLogger(__name__)

# Prevent debug output from propagating
peewee_logger = logging.getLogger("peewee")
peewee_logger.setLevel(logging.INFO)

# Init'd later in the PeeweeStorage constructor.
#   See: http://docs.peewee-orm.com/en/latest/peewee/database.html#run-time-database-configuration
# Another option would be to use peewee's Proxy.
#   See: http://docs.peewee-orm.com/en/latest/peewee/database.html#dynamic-db
_db = SqliteExtDatabase(None)


LATEST_VERSION = 2


def chunks(l, n):
    """Yield successive n-sized chunks from l.
    From: https://stackoverflow.com/a/312464/965332"""
    for i in range(0, len(l), n):
        yield l[i : i + n]


class BaseModel(Model):
    class Meta:
        database = _db


class BucketModel(BaseModel):
    key = IntegerField(primary_key=True)
    id = CharField(unique=True)
    created = DateTimeField(default=datetime.now)
    name = CharField(null=True)
    type = CharField()
    client = CharField()
    hostname = CharField()

    def json(self):
        return {
            "id": self.id,
            "created": iso8601.parse_date(self.created)
            .astimezone(timezone.utc)
            .isoformat(),
            "name": self.name,
            "type": self.type,
            "client": self.client,
            "hostname": self.hostname,
        }


class EventModel(BaseModel):
    id = AutoField()
    bucket = ForeignKeyField(BucketModel, backref="events", index=True)
    timestamp = DateTimeField(index=True, default=datetime.now)
    duration = DecimalField()
    datastr = CharField()

    @classmethod
    def from_event(cls, bucket_key, event: Event):
        return cls(
            bucket=bucket_key,
            id=event.id,
            timestamp=event.timestamp,
            duration=event.duration.total_seconds(),
            datastr=json.dumps(event.data),
        )

    def json(self):
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "duration": float(self.duration),
            "data": json.loads(self.datastr)
        }

    def customjson(self):
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "duration": float(self.duration),
            "data": json.loads(self.datastr),
            "bucket": self.bucket.id,
            "type":self.bucket.type
        }

class ApplicationsModel(BaseModel):
    id = AutoField()
    applicaionname = CharField()

    def json(self):
        return {
            "id": self.id,
            "applicaionname": self.applicaionname
        }


class SummaryModel(BaseModel):
    id = AutoField()
    bucket = ForeignKeyField(BucketModel, backref="summary", index=True)
    application = ForeignKeyField(ApplicationsModel, backref="application", index=True)
    timestamp = DateField(index=True, default=datetime.now)
    duration = DecimalField()

    def json(self):
        return {
            "id": self.id,
            "timestamp": iso8601.parse_date(str(self.timestamp))
            .astimezone(timezone.utc)
            .isoformat(),
            "duration": float(self.duration),
            "application": self.application.applicaionname,
            "bucket": self.bucket.hostname,
        }
    
# class ApplicationCategoryModel(BaseModel):
#     id = AutoField()
#     categoryname = CharField()

#     def json(self):
#         return {
#             "id": self.id,
#             "categoryname": self.categoryname
#         }


# class ApplicationCategoryMappingModel(BaseModel):
#     id = AutoField()
#     category = ForeignKeyField(ApplicationCategoryModel, backref="categoryname", index=True)
#     application = ForeignKeyField(ApplicationsModel, backref="applicaionname", index=True)

#     def json(self):
#         return {
#             "id": self.id,
#             "category": self.category
#         }



class PeeweeStorage(AbstractStorage):
    sid = "peewee"

    def __init__(self, testing: bool = True, filepath: str = None) -> None:
        data_dir = get_data_dir("aw-server")

        if not filepath:
            filename = (
                "peewee-sqlite"
                + ("-testing" if testing else "")
                + ".v{}".format(LATEST_VERSION)
                + ".db"
            )
            filepath = os.path.join(data_dir, filename)
        self.db = _db
        self.db.init(filepath)
        logger.info("Using database file: {}".format(filepath))

        self.db.connect()

        self.bucket_keys: Dict[str, int] = {}
        self.applications_keys: Dict[str, int] = {}
        BucketModel.create_table(safe=True)
        EventModel.create_table(safe=True)
        ApplicationsModel.create_table(safe=True)
        SummaryModel.create_table(safe=True)
        # ApplicationCategoryModel.create_table(safe=True)
        # ApplicationCategoryMappingModel.create_table(safe=True)
        self.update_bucket_keys()
        self.update_application_keys()

    def update_bucket_keys(self) -> None:
        buckets = BucketModel.select()
        self.bucket_keys = {bucket.id: bucket.key for bucket in buckets}
        

    def buckets(self) -> Dict[str, Dict[str, Any]]:
        buckets = {bucket.id: bucket.json() for bucket in BucketModel.select()}
        return buckets

    def update_application_keys(self) -> None:
        applications = ApplicationsModel.select()
        self.applications_keys = {application.applicaionname: application.id for application in applications}
        

    def applications(self) -> Dict[str, Dict[str, Any]]:
        applications = {application.id: application.json() for bucket in ApplicationsModel.select()}
        return applications

    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: Optional[str] = None,
    ):
        BucketModel.create(
            id=bucket_id,
            type=type_id,
            client=client,
            hostname=hostname,
            created=created,
            name=name,
        )
        self.update_bucket_keys()

    def delete_bucket(self, bucket_id: str) -> None:
        if bucket_id in self.bucket_keys:
            EventModel.delete().where(
                EventModel.bucket == self.bucket_keys[bucket_id]
            ).execute()
            BucketModel.delete().where(
                BucketModel.key == self.bucket_keys[bucket_id]
            ).execute()
            self.update_bucket_keys()
        else:
            raise Exception("Bucket did not exist, could not delete")

    def get_metadata(self, bucket_id: str):
        if bucket_id in self.bucket_keys:
            return BucketModel.get(
                BucketModel.key == self.bucket_keys[bucket_id]
            ).json()
        else:
            raise Exception("Bucket did not exist, could not get metadata")

    def insert_one(self, bucket_id: str, event: Event) -> Event:
        e = EventModel.from_event(self.bucket_keys[bucket_id], event)
        e.save()
        b = BucketModel.get(
                BucketModel.key == self.bucket_keys[bucket_id]
            )
        # print(event)
        self.insert_update_customTables(event.data,b.type,e,bucket_id,e.timestamp,e.duration,b,False)
        event.id = e.id
        return event
    
    def reloadAll(self, start, end):
        print("LOADING...")
        events = EventModel.select().join(BucketModel).paginate(int(start), int(end))
        for event in list(map(EventModel.customjson, events.execute())):
            print(event)
            b = BucketModel.get(
                BucketModel.key == self.bucket_keys[event["bucket"]]
            )
            self.insert_update_customTables(event["data"],event["type"],event,event["bucket"],iso8601.parse_date(str(event["timestamp"])),event["duration"],b,False)
            
        return "SUCCESS"

    def select_custom(self):
        s = {summary.id: summary.json() for summary in SummaryModel.select()}
        return s
    
    def get_custom_events(self, start: datetime = None, end: datetime = None):
        if start:
            # Important to normalize datetimes to UTC, otherwise any UTC offset will be ignored
            start = start.astimezone(timezone.utc)
        if end:
            end = end.astimezone(timezone.utc)
        # print(start)
        # print(end)
        # print(SummaryModel.select().join(BucketModel).where((start <= SummaryModel.timestamp) & (SummaryModel.timestamp <= end) & (BucketModel.type == 'currentwindow')).sql())
        # print(ApplicationsModel.select(ApplicationsModel.applicaionname,ApplicationCategoryModel.categoryname,SummaryModel.duration,SummaryModel.timestamp,SummaryModel.bucket).join(ApplicationCategoryMappingModel).join(ApplicationCategoryModel).join(SummaryModel,on=(ApplicationsModel.id==SummaryModel.application), attr='log').join(BucketModel,on=(ApplicationsModel.id==SummaryModel.application), attr='log').sql())
        s = {summary.id: summary.json() for summary in SummaryModel.select().join(BucketModel).where((start <= SummaryModel.timestamp) & (SummaryModel.timestamp <= end)).execute()}
        return s

    def insert_update_customTables(self, data, buckettype, e, bucket_id, timestamp, duration, b,replace):
        datakeys = data.keys()
        applicationName = None
        
        if 'app' in datakeys:
            applicationName = data["app"]
        if 'status' in datakeys:
            # print(data, "---", duration)
            applicationName = 'GLOBALACTIVITY'
            
        # print(bucket_id, "---", applicationName, "---", duration)
        if 'status' in datakeys:
            fromDate = datetime(timestamp.year, timestamp.month, timestamp.day)
            toDate = datetime.combine(datetime(timestamp.year, timestamp.month, timestamp.day), time(23, 59, 59, 999999))
            events = EventModel.select(fn.SUM(EventModel.duration).alias('total')).where(
                        (EventModel.timestamp >= fromDate) & (EventModel.timestamp <= toDate) & (EventModel.datastr.contains('not-afk'))
                    )
            
            # print(sql.replace("?","{}").format(*param))
            if events is not None:
                duration = 0
                duration = events.dicts()[0].get('total')
        print(bucket_id,"---",applicationName,"---",duration)
        if applicationName is not None:
            # print("-----",self.applications_keys.get(applicationName))
            if self.applications_keys.get(applicationName) is None:
                applicaion = ApplicationsModel.create(
                    applicaionname=applicationName
                )
                applicaion.save()
                self.update_application_keys()
            else:
                applicaion = ApplicationsModel.get_or_none(
                    ApplicationsModel.applicaionname == applicationName
                )
                
            # print("------------",applicaion,duration)
            if(applicaion!=None):
                summaryData = SummaryModel.select().join(BucketModel).switch(SummaryModel).join(ApplicationsModel).where(
                    (SummaryModel.timestamp == timestamp.date()) & (BucketModel.key == self.bucket_keys[bucket_id]) & (ApplicationsModel.applicaionname==applicationName)
                )
                sql, param = summaryData.sql()
                print(sql.replace("?","{}").format(*param))
                if(summaryData.count()<=0):
                    s = SummaryModel.create(
                        timestamp= timestamp.date(),
                        duration=float(duration),
                        application= applicaion,
                        bucket=b
                    )
                    # s.save()
                else:
                    for sumdata in summaryData:
                        if applicationName == 'GLOBALACTIVITY':
                            sumdata.duration = float(duration)
                        else:
                            sumdata.duration = float(sumdata.duration) + float(duration)
                        sumdata.save()
        
        
        # self.insert_globalDatas(b,bucket_id,timestamp,duration,'GLOBALACTIVITY')
        # self.insert_globalDatas(b,bucket_id, timestamp, duration, 'AFK')
        # self.insert_globalDatas(b,bucket_id,timestamp,duration,'NOTAFK')


    def insert_globalDatas(self,b,bucket_id,timestamp,duration,applicationname):
        applicaionname = applicationname
        if self.applications_keys.get(applicaionname) is None:
            applicaion = ApplicationsModel.create(
                applicaionname=applicaionname
            )
            # application.save()
            self.update_application_keys()
        else:
            applicaion = ApplicationsModel.get_or_none(
                ApplicationsModel.applicaionname == applicaionname
            )
        print(applicaionname,duration)
        if(applicaion!=None):
            summaryData = SummaryModel.select().join(BucketModel).switch(SummaryModel).join(ApplicationsModel).where(
                (SummaryModel.timestamp == timestamp.date()) & (BucketModel.key == self.bucket_keys[bucket_id]) & (ApplicationsModel.applicaionname==applicaionname)
            )
            if(summaryData.count()<=0):
                s = SummaryModel.create(
                    timestamp= timestamp.date(),
                    duration=float(duration),
                    application= applicaion,
                    bucket=b
                )
                s.save()
            else:
                fromDate = datetime(timestamp.year, timestamp.month, timestamp.day)
                toDate = datetime.combine(datetime(timestamp.year, timestamp.month, timestamp.day), time(23, 59, 59, 999999))
                # print(fromDate)
                # print(toDate)
                events = None
                if applicationname=='GLOBALACTIVITY':
                    events = EventModel.select(fn.SUM(EventModel.duration).alias('total')).where(
                        (EventModel.timestamp >= fromDate) & (EventModel.timestamp <= toDate)
                    )
                    print(EventModel.select().where(
                        (EventModel.timestamp >= fromDate) & (EventModel.timestamp <= toDate)
                    ).sql())
                elif applicationname=='AFK':
                    events = EventModel.select(fn.SUM(EventModel.duration).alias('total')).where(
                        (EventModel.timestamp >= fromDate) & (EventModel.timestamp <= toDate) &
                        ((EventModel.datastr.contains('afk'))) & (~(EventModel.datastr.contains('not-afk')))
                    )
                elif applicationname=='NOTAFK':
                    events = EventModel.select(fn.SUM(EventModel.duration).alias('total')).where(
                        (EventModel.timestamp >= fromDate) & (EventModel.timestamp <= toDate) &
                        ((EventModel.datastr.contains('not-afk')))
                    )
                if events is not None:
                    duration = 0
                    # print(events.sql())
                    duration = events.dicts()[0].get('total')
                    if duration is not None:
                        for sumdata in summaryData:
                            if applicationname == 'GLOBALACTIVITY':
                                now = datetime.now().astimezone(timezone.utc)
                                now = now.replace(hour=0, minute=0, second=0, microsecond=0)
                                duration = (timestamp.astimezone(timezone.utc)-now).total_seconds()
                                if (float(duration) > float(sumdata.duration)):
                                    sumdata.duration = float(duration)
                            else:
                                sumdata.duration = float(duration)
                            sumdata.save()

    def insert_many(self, bucket_id, events: List[Event], fast=False) -> None:
        events_dictlist = [
            {
                "bucket": self.bucket_keys[bucket_id],
                "timestamp": event.timestamp,
                "duration": event.duration.total_seconds(),
                "datastr": json.dumps(event.data),
            }
            for event in events
        ]
        # Chunking into lists of length 100 is needed here due to SQLITE_MAX_COMPOUND_SELECT
        # and SQLITE_LIMIT_VARIABLE_NUMBER under Windows.
        # See: https://github.com/coleifer/peewee/issues/948
        for chunk in chunks(events_dictlist, 100):
            EventModel.insert_many(chunk).execute()
        for event in events_dictlist:
            b = BucketModel.get(
                BucketModel.key == self.bucket_keys[event["bucket"]]
            )
            # print(event)
            self.insert_update_customTables(json.load(event["datastr"]),b.type,e,event["bucket"],iso8601.parse_date(str(event["timestamp"])),event["duration"],b,False)

    def _get_event(self, bucket_id, event_id) -> EventModel:
        return (
            EventModel.select()
            .where(EventModel.id == event_id)
            .where(EventModel.bucket == self.bucket_keys[bucket_id])
            .get()
        )

    def _get_last(self, bucket_id) -> EventModel:
        return (
            EventModel.select()
            .where(EventModel.bucket == self.bucket_keys[bucket_id])
            .order_by(EventModel.timestamp.desc())
            .get()
        )

    def replace_last(self, bucket_id, event):
        e = self._get_last(bucket_id)
        e.timestamp = event.timestamp
        # print(float(e.duration),float(event.duration.total_seconds()))
        print("REPLACE")
        if(float(e.duration)>0):
            duration = float(e.duration) - float(event.duration.total_seconds())
        e.duration = event.duration.total_seconds()
        e.datastr = json.dumps(event.data)
        e.save()
        event.id = e.id
        b = BucketModel.get(
                BucketModel.key == self.bucket_keys[bucket_id]
            )
        # print(event)
        self.insert_update_customTables(event.data,b.type,e,bucket_id,e.timestamp,event.duration.total_seconds(),b,True)
        return event

    def delete(self, bucket_id, event_id):
        return (
            EventModel.delete()
            .where(EventModel.id == event_id)
            .where(EventModel.bucket == self.bucket_keys[bucket_id])
            .execute()
        )

    def replace(self, bucket_id, event_id, event):
        e = self._get_event(bucket_id, event_id)
        e.timestamp = event.timestamp
        e.duration = event.duration.total_seconds()
        e.datastr = json.dumps(event.data)
        e.save()
        event.id = e.id
        b = BucketModel.get(
                BucketModel.key == self.bucket_keys[bucket_id]
            )
        # print(event)
        self.insert_update_customTables(event.data,b.type,e,bucket_id,e.timestamp,event.duration.total_seconds(),b,True)
        return event

    def get_events(
        self,
        bucket_id: str,
        limit: int,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ):
        if limit == 0:
            return []
        q = (
            EventModel.select()
            .where(EventModel.bucket == self.bucket_keys[bucket_id])
            .order_by(EventModel.timestamp.desc())
            .limit(limit)
        )
        if starttime:
            # Important to normalize datetimes to UTC, otherwise any UTC offset will be ignored
            starttime = starttime.astimezone(timezone.utc)
            q = q.where(starttime <= EventModel.timestamp)
        if endtime:
            endtime = endtime.astimezone(timezone.utc)
            q = q.where(EventModel.timestamp <= endtime)
        return [Event(**e) for e in list(map(EventModel.json, q.execute()))]

    def get_eventcount(
        self,
        bucket_id: str,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ):
        q = EventModel.select().where(EventModel.bucket == self.bucket_keys[bucket_id])
        if starttime:
            # Important to normalize datetimes to UTC, otherwise any UTC offset will be ignored
            starttime = starttime.astimezone(timezone.utc)
            q = q.where(starttime <= EventModel.timestamp)
        if endtime:
            endtime = endtime.astimezone(timezone.utc)
            q = q.where(EventModel.timestamp <= endtime)
        return q.count()
