import json
import sys
import os
import time
import urllib.parse
from datetime import datetime, timezone, timedelta
from gzip import GzipFile
from io import BytesIO
from time import sleep

import boto3

from lib.ctgrazer.ConfigUtil import ConfigUtil
from lib.ctgrazer.SendMessage import SendMessage
from lib.ctgrazer.Constants import Constants, Level, ThreadLevel, EventMeta


class ConfigValidationError(Exception): pass

cfg = None
MASTER_CONFIGURATION_FILE = "config.ini"


def initialize():
    print("[INFO] Initializing ctgrazer...")
    # Instantiate Configuration Utility Class
    config = ConfigUtil()

    # Logic to open the file and load the config dict
    # Process is terminated if the file is not found or IO error
    if os.path.exists(MASTER_CONFIGURATION_FILE):
        try:
            config_file = open(MASTER_CONFIGURATION_FILE)

            # Load master configuration file
            config.load(config_file)
        except IOError as error:
            print("[Error] Process terminated: {}".format(error))
            sys.exit(1)
    else:
        print("[Error] Process Terminated: File not found.")
        sys.exit(1)

    config.set_valid_status(True) # set the value to true

    # Validate Mandatory Parameters
    # Process is terminated if the mandatory section and parameters does not exist
    if config.is_section(ConfigUtil.SECTION_REQUIRED):

        missing_parameters = []
        missing_parameter_value = []

        for key in ConfigUtil.REQUIRED_PARAMETERS_LIST:

            if config.is_required_key_exists(key):         # Check if the key exist in the section
                param_value = config.config[ConfigUtil.SECTION_REQUIRED][key]

                if not param_value:                     # if the param is blank, report as error
                    missing_parameter_value.append(key)

            else:
                missing_parameters.append(key)

        if missing_parameters:
            print("[ERROR] Mandatory Parameter {} does not exist.".format(missing_parameters))
            config.set_valid_status(False)

        if missing_parameter_value:
            print("[ERROR] Mandatory Parameter value for the keys {} is None.".format(missing_parameter_value))
            config.set_valid_status(False)

    else:
        print("[ERROR] Missing [REQUIRED PARAMETERS]. Required Parameters are: {}".format(ConfigUtil.REQUIRED_PARAMETERS_LIST))
        config.set_valid_status(False)


    # Evaluate [OPTIONAL PARAMTERES}; if not present, use Default. (ConfigUtil.OPTIONAL_PARAMETERS_DICT)
    if config.is_section(ConfigUtil.SECTION_OPTIONAL):

        for key in ConfigUtil.OPTIONAL_PARAMETERS_DICT.keys():

            if not (config.is_optional_key_exists(key) and config.config[ConfigUtil.SECTION_OPTIONAL][key]):
                config.set_optional(key, ConfigUtil.OPTIONAL_PARAMETERS_DICT[key][ConfigUtil.KEY_VALUE])

    else:
        config.add_optional_section()

    # Complete Initialization
    config.complete_init()

    if not config.is_valid():
        print("[INFO] ctgrazer initialization incomplete.")
        return None

    print("[INFO] ctgrazer initialization completed.")

    return config


def processObject(bucket, objectKey, sourcename, Logger):
    retries = 3
    count = 0

    Logger.sendEvent('Processing Obj: ' + sourcename)

    s3 = boto3.resource(Constants.AWS_S3)
    object = s3.Object(bucket, objectKey)

    # in case the object key isn't there yet; sleep a little and check again
    # sometimes the notification beats the object
    for attempt in range(retries + 1):
        try:
            response = object.get()
        except:
            if attempt < (retries):
                sleep(10)
                Logger.sendEvent('Get Object Warning Retry A Attempt:' + str(attempt + 1), severity=Level.WARNING)
                continue
            else:
                Logger.sendEvent('Retries:' + str(retries) + ' exhausted.  Can\'t Get Object:' + objectKey + ' From BUCKET:' + bucket, severity=Level.CRITICAL)
                # finish all writes to the log, and force a lambda restart by sending
                # non-zero return code
                Logger.kill()
                sys.exit(0)
        else:
            # we got the object, move forward
            break

    # Sometimes the stream times out, may be the object hasn't fully copied, if so retry
    retries = 3
    for attempt in range(retries + 1):
        try:
            data = BytesIO(response['Body'].read())
        except:
            if attempt < retries:
                Logger.sendEvent('Attempt to Stream Data Failed, Attempt Number:' + str(attempt + 1), severity=Level.WARNING)
                # we will retry so need to re-get the object
                sleep(5)
                try:
                    response = object.get()
                except:
                    Logger.sendEvent('Can\'t Get Object Stream Attempt, Obj: ' + objectKey + ' From BUCKET:' + bucket, severity=Level.CRITICAL)
                    Logger.kill()
                    sys.exit(0)
            else:
                Logger.sendEvent('Retries:' + str(retries) + ' exhausted.  Can\'t Stream Obj: ' + objectKey + ' From BUCKET:' + bucket, severity=Level.CRITICAL)
                # we did our best, 3 strikes and your out.
                # cause a invocation error and retry
                Logger.kill()
                sys.exit(0)
        else:
            break
    dcfile = json.loads(GzipFile(fileobj=data, mode='rb').read().decode(Constants.ENCODING_UTF))

    for record in dcfile[Constants.AWS_RECORDS]:
        count += 1
        payload = {}
        payload.update({EventMeta.SOURCE_TYPE.value: cfg.get_config_value(ConfigUtil.KEY_SOURCE_TYPE)})
        payload.update({EventMeta.SOURCE.value: sourcename})
        payload.update({EventMeta.TIME.value: time.mktime(datetime.strptime(record[Constants.AWS_EVENT_TIME], Constants.TIME_STAMP_YMDHMZ).timetuple())})
        payload.update({EventMeta.EVENT.value: record})
        Logger.batchEvent(payload)

        if Logger.isError():
            # in case the error is with HEC, print to CloudWatch as well
            print('Error Reason:' + Logger.errorMessage)
            Logger.sendEvent('Error Reason:' + Logger.errorMessage, severity=Level.ERROR)
            return

    # log the number of CT events in the object
    Logger.sendEvent('Events processed: ' + str(count))

    # remove the object if load was fine
    try:
        object.delete()
    except:
        Logger.sendEvent('Unable to Remove Obj:' + sourcename, severity=Level.ERROR)
    else:
        Logger.sendEvent('Removed Obj: ' + sourcename)


def determine_thread_size(size):
    # based on size of object, create set the number of threads
    number_of_threads = 11

    if size <= ThreadLevel.LEVEL_1.value:
        number_of_threads = 3
    elif size <= ThreadLevel.LEVEL_2.value:
        number_of_threads = 5
    elif size <= ThreadLevel.LEVEL_3.value:
        number_of_threads = 7
    elif size <= ThreadLevel.LEVEL_4.value:
        number_of_threads = 9

    return number_of_threads


def lambda_handler(event, context):

    # Initialize ctgrazer configuration
    global cfg
    cfg = initialize()

    if not cfg or not cfg.is_valid():
        raise ConfigValidationError('Please handle config error.')

    obj_list = []

    if Constants.AWS_RECORDS in event.keys() and event[Constants.AWS_RECORDS][0][Constants.AWS_EVENT_SRC] == 'aws:s3':

        bucket = event[Constants.AWS_RECORDS][0][Constants.AWS_S3][Constants.AWS_BUCKET][Constants.AWS_NAME]
        called_method = Constants.GRAZER_EVENT_S3_PUT

        obj_list.append(urllib.parse.unquote_plus(event[Constants.AWS_RECORDS][0][Constants.AWS_S3][Constants.AWS_OBJECT][Constants.AWS_KEY], encoding=Constants.ENCODING_UTF))
        size = event[Constants.AWS_RECORDS][0][Constants.AWS_S3][Constants.AWS_OBJECT][Constants.AWS_SIZE]

        number_of_threads = determine_thread_size(size)

        # Set thread size based on the file size for s3 put trigger
        cfg.config[ConfigUtil.KEY_BATCH_THREAD_SIZE] = number_of_threads

    elif Constants.AWS_DETAIL_TYPE in event.keys() and event['source']=='aws.events':
        # this means we were called via scheduled event, so see if any old files need
        # to be retrieved

        called_method = event[Constants.AWS_DETAIL_TYPE]
        s3 = boto3.resource(Constants.AWS_S3)

        bucket = cfg.get_config_value(ConfigUtil.KEY_S3_BUCKET)
        prefix = cfg.get_config_value(ConfigUtil.KEY_S3_BUCKET_PREFIX)

        bucket_instance = s3.Bucket(bucket)
        objs = bucket_instance.objects.filter(Prefix=prefix)

        for obj in objs:
            object = s3.Object(bucket, obj.key)
            difference = (datetime.now(timezone.utc) - object.last_modified) / timedelta(minutes=1)

            if difference > int(cfg.get_config_value(ConfigUtil.KEY_MINS_TO_PROCESS)):
                obj_list.append(obj.key)

        # just set to default of 5
        number_of_threads = cfg.get_config_value(ConfigUtil.KEY_BATCH_THREAD_SIZE)
        size = 'N/A'

    else:
        print(Constants.GRAZER_EVENT_UNHANDLED)
        sys.exit(1)

    logger = SendMessage(context, cfg)
    # if we could not create the object, then just print to cloudWatch all objects not processed
    # based on size of object, create set the number of threads
    logger.sendEvent('Called via:' + called_method)

    logger.sendEvent('Number of threads Requested:' + str(number_of_threads) + ' Size:' + str(size))

    for key in obj_list:
        # make call for each object
        sourcename = 's3://' + bucket + '/' + key
        processObject(bucket,
                      key,
                      sourcename,
                      logger
                     )

        if logger.isError():
            print('Not Fully Processed Obj:' + sourcename + ' Reason:' + logger.errorMessage)
            logger.sendEvent('Not Fully Processed Obj:' + sourcename + ' Reason:' + logger.errorMessage, severity=Level.CRITICAL)
            break

    logger.kill()
