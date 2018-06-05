from enum import Enum, unique


class Constants:

    TIME_STAMP_YMDHMZ = '%Y-%m-%dT%H:%M:%SZ'
    MSG = 'message'
    TXT = 'text'
    REASON = 'reason'
    CODE = 'code'
    COUNT = 'count'

    DEST_SPLUNK = 'SPLUNK'
    DEST_CLOUDWATCH = 'CLOUDWATCH'

    START_MESSAGE_FORMAT = 'Time_ms=%s START RequestId=%s Severity=%s Function=%s Version=%s Timeout=%d MemoryLimit=%s MB Stream=%s'
    STOP_MESSAGE_FORMAT = 'Time_ms=%d STOP RequestId=%s Severity=%s RemainingTime=%d RunTime=%s ms'

    MAX_BYTE_LENGTH = 100000

    ENCODING_UTF = 'utf-8'

    AWS_RECORDS = 'Records'
    AWS_EVENT_SRC = 'eventSource'
    AWS_EVENT_TIME = 'eventTime'
    AWS_S3 = 's3'
    AWS_BUCKET = 'bucket'
    AWS_NAME = 'name'
    AWS_SIZE = 'size'
    AWS_KEY = 'key'
    AWS_DETAIL_TYPE = 'detail-type'
    AWS_OBJECT = 'object'

    GRAZER_EVENT_S3_PUT = 's3 Put Trigger'
    GRAZER_EVENT_SCHEDULED = 'Scheduled Event' #TODO: NOT SURE ABOUT THIS!
    GRAZER_EVENT_UNHANDLED = 'Unhandled Event'


@unique
class Level(Enum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50

    def describe(self):
        return self.name, self.value

    def __str__(self):
        return str(self.value)

    @classmethod
    def default(cls):
        return cls.INFO


@unique
class EventMeta(Enum):
    SOURCE_TYPE = 'sourcetype'
    SOURCE = 'source'
    TIME = 'time'
    EVENT = 'event'
    HOST = 'host'

    def describe(self):
        return self.name, self.value


@unique
class ThreadLevel(Enum):
    LEVEL_1 = 10500
    LEVEL_2 = 253474
    LEVEL_3 = 528241
    LEVEL_4 = 1086698

    def describe(self):
        return self.name, self.value

    def __str__(self):
        return str(self.value)
