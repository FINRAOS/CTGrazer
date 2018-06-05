import time
import json
import lib.requests as requests
import threading
import logging
import urllib.parse as urlparse
from lib.ctgrazer.ConfigUtil import ConfigUtil
from lib.ctgrazer.Constants import Constants, Level, EventMeta

try:
    import Queue
except ImportError:
    import queue as Queue
from time import sleep

# create a local for the threads to log errors
error_lock = threading.Lock()


class SendMessage:

    def __init__(self,
                 context,
                 config
                ):

        # Set some variables from context object
        self.config            = config
        self.start             = context.get_remaining_time_in_millis()
        self.startms           = str(int(round(time.time() * 1000)))
        self.memory_limit      = str(context.memory_limit_in_mb)
        self.request_id        = context.aws_request_id
        self.function_arn      = context.invoked_function_arn
        self.function_name     = context.function_name
        self.log_group         = context.log_group_name
        self.log_stream_name   = context.log_stream_name
        self.function_version  = context.function_version
        self.context           = context

        # Used for collecting batch events
        self.batchEvents       = []
        self.currentByteLength = 0
        self.maxByteLength     = Constants.MAX_BYTE_LENGTH
        self.errorMessage      = ''

        self.debug = self.config.get_config_value(ConfigUtil.KEY_DEBUG)
        self.number_of_threads = self.config.get_config_value(ConfigUtil.KEY_BATCH_THREAD_SIZE)
        self.message_prefix = self.config.get_config_value(ConfigUtil.KEY_LOG_MSG_PREFIX)
        destination = self.config.get_config_value(ConfigUtil.KEY_LOG_DESTINATION)

        self.http_event_collector_key = self.config.get_config_value(ConfigUtil.KEY_SPLUNK_HEC_KEY)
        self.http_event_collector_server_uri = self.config.get_config_value(ConfigUtil.KEY_SPLUNK_HEC_ENDPOINT)
        self.source_type = self.config.get_config_value(ConfigUtil.KEY_SOURCE_TYPE)
        self.debug_source_type = self.config.get_config_value(ConfigUtil.KEY_SPLUNK_DEBUG_SOURCETYPE)
        self.http_event_collector_host = self._getHostName(self.http_event_collector_server_uri)

        # create a logger instance in case anything needs to go to CloudWatch
        self.cw_logger = logging.getLogger()
        if(self.debug):
            self.cw_logger.setLevel(logging.DEBUG)
        else:
            self.cw_logger.setLevel(logging.INFO)

        self.destination = self._determineWhereToLog(destination)

        # if we are writing to splunk, create the http object
        if self._isSplunk():
            self.httpObject = _http_logger( self.http_event_collector_key,
                                            self.http_event_collector_server_uri,
                                            self.cw_logger,
                                            number_of_threads  = self.number_of_threads,
                                            maxByteLength      = self.maxByteLength,
                                            debug              = self.debug
                                          )

            # if issue with setting up object (threading/queuing) revert back to CW
            if( self.httpObject.error[Constants.COUNT] > 0):
                self.cw_logger.error('Could not Create http Object reason:'+self.httpObject.error[Constants.MSG][Constants.REASON])
                self.destination = Constants.DEST_CLOUDWATCH

        # ok, we know where to send, so create a START record for the session
        self._sendStartRecord()

    def kill(self):
        if self._isSplunk():
            # if there is still some batch events to send, do it
            if( len(self.batchEvents) > 0 ):
                self.httpObject._sendEvent(self.batchEvents)
            # we are done' send the stop record
        self._sendStopRecord()
        if self._isSplunk():
            # now just wait for the threads to finish, kill the queues, then suicide
            self.httpObject._waitUntilDone()
            del self.httpObject
        del self

    def isError(self):
        if( self.httpObject.error[Constants.COUNT] > 0):
            msg = self.httpObject.error.get(Constants.MSG, {})
            self.errorMessage = 'TEXT:' + str(msg.get(Constants.TXT, 'NO_VALUE')) + ' CODE:' + str(msg.get(Constants.CODE, 'NO_VALUE')) + ' REASON:' + str(msg.get(Constants.REASON, 'NO_VALUE'))
            self.cw_logger.error('ERROR IN SENDING DATA'+self.errorMessage)
            return True
        else:
            return False

    def _sendStartRecord(self):
        message = Constants.START_MESSAGE_FORMAT % (self.startms, self.request_id, Level.INFO, self.function_arn, self.function_version, self.start, self.memory_limit, self.log_stream_name)
        if self._isSplunk():
            event = []
            event.append(self._packageEvent(message))
            self.httpObject._sendEvent(event)
        else:
          self.cw_logger.info(message)

    def _sendStopRecord(self):
        elapsedTime = int(round(time.time() * 1000)) - int(self.startms)
        message = Constants.STOP_MESSAGE_FORMAT % (int(round(time.time() * 1000)), self.request_id, Level.INFO, self.context.get_remaining_time_in_millis(), elapsedTime)
        if self._isSplunk():
            event = []
            event.append(self._packageEvent(message))
            self._packageEvent(message)
            self.httpObject._sendEvent(event)
        else:
            self.cw_logger.info(message)

    def sendEvent(self,
                  payload,
                  severity = Level.INFO):

        if(isinstance(payload, str)):
            if( len(payload) <= 0):
                self.cw_logger.error('EMPTY STRING MESSAGE SENT')
                return
            severity = self._determineSeverity(severity)
            message = self.message_prefix % (int(round(time.time() * 1000)), self.request_id, severity, payload )
            if self._isSplunk():
                event = []
                event.append(self._packageEvent(message))
                self.httpObject._sendEvent(event)
            else:
                self._sendToCloudWatch(self, severity, message)
        elif(isinstance(payload, dict)):
            if(len(payload) <= 0):
                self.cw_logger.error('EMPTY DICTIONARY MESSAGE SENT')
                return
            if( self._validateDictonary(payload) ):
                event = []
                event.append(json.dumps(payload))
                self.httpObject._sendEvent(event)
            else:
                self.cw_logger.error('PAYLOAD DID NOT PASS VALIDATION')
        else:
            self.cw_logger.error('Invalid Type Sent:'+type(payload))

    def _sendToCloudWatch(self,severity, message):
       if severity == Level.DEBUG:
            self.cw_logger.debug(message)
       elif severity == Level.INFO:
            self.cw_logger.info(message)
       elif severity == Level.WARNING:
            self.cw_logger.warning(message)
       elif severity == Level.ERROR:
            self.cw_logger.error(message)
       elif severity == Level.CRITICAL:
            self.cw_logger.critical(message)
       else:
            self.cw_logger.info(message)

    def batchEvent(self,
                   payload,
                   severity=Level.INFO):

        if(isinstance(payload, str)):
            if(len(payload) <= 0):
                self.cw_logger.error('EMPTY STRING BATCH MESSAGE SENT')
                return
            severity = self._determineSeverity(severity)
            message = self.message_prefix % (int(round(time.time() * 1000)), self.request_id, severity, payload)
            if self._isSplunk():
                payLoadString = self._packageEvent(message)
                payLoadLength = len(payLoadString)

                # if the new event pushes us over the max OR
                # the array is empty (avoid sending 0 events
                if( ( ( self.currentByteLength+payLoadLength ) > self.maxByteLength ) and
                    ( len(self.batchEvents)  != 0 ) ):
                    # This will push us over the limit, so send the array of dictionaries to splunk
                    self.httpObject._sendEvent(self.batchEvents.copy())
                    self.batchEvents       = []
                    self.currentByteLength = 0
                self.batchEvents.append(payLoadString)
                self.currentByteLength += payLoadLength
            else:
                self._sendToCloudWatch(self, severity, message)
        elif(isinstance(payload, dict)):
            if(len(payload) <= 0):
                self.cw_logger.error('EMPTY DICTIONARY BATCH MESSAGE SENT')
                return
            if( self._validateDictonary(payload) ):
                payLoadString = json.dumps(payload)
                payLoadLength = len(payLoadString)

                if( ( ( self.currentByteLength+payLoadLength ) > self.maxByteLength ) and
                    ( len(self.batchEvents)  != 0 ) ):
                    # This will push us over the limit, so send the array of dictionaries to splunk
                    self.httpObject._sendEvent(self.batchEvents.copy())
                    self.batchEvents       = []
                    self.currentByteLength = 0
                self.batchEvents.append(payLoadString)
                self.currentByteLength += payLoadLength
            else:
                self.cw_logger.error('PAYLOAD DID NOT PASS VALIDATION')
        else:
            self.cw_logger.error('NOT A VALID PAYLOAD FORMAT: {}'.format(type(payload)))

    def _validateDictonary(self, payload):
        if EventMeta.HOST.value not in payload:
            payload.update({EventMeta.HOST.value: self.http_event_collector_host })
        if EventMeta.TIME.value not in payload:
            eventtime = str(int(round(time.time() * 1000)))
            payload.update({EventMeta.TIME.value: eventtime })
        if EventMeta.SOURCE.value not in payload:
            self.cw_logger.error('No SOURCE found in INPUT')
            return False
        if EventMeta.SOURCE_TYPE.value not in payload:
            self.cw_logger.error('No SOURCETYPE found in INPUT')
            return False

        return True

    def _packageEvent(self, message):
        payload={}
        payload.update({EventMeta.SOURCE_TYPE.value: self.debug_source_type})
        payload.update({EventMeta.SOURCE.value: self.function_arn})
        eventtime = str(int(round(time.time() * 1000000)))
        payload.update({EventMeta.TIME.value: eventtime})
        payload.update({EventMeta.HOST.value: self.http_event_collector_host})
        payload.update({EventMeta.EVENT.value: message})
        return json.dumps(payload)

    def _determineSeverity(self, severity):
        if not Level.__contains__(severity):
            return Level.INFO

        return severity;

    def _determineWhereToLog(self, destination):
        # default to cloudwatch if invalid destination sent
        if( destination not in [Constants.DEST_SPLUNK, Constants.DEST_CLOUDWATCH]):
            self.cw_logger.error('INVALID DESTINATION:'+destination+' Sent, reverting to CLOUDWATCH')
            return Constants.DEST_CLOUDWATCH
        return destination

    def _isSplunk(self):
        return self.destination == Constants.DEST_SPLUNK

    def _getHostName(self, uri):
        result = urlparse.urlparse(uri)
        return result.hostname

class _http_logger:
    def __init__( self,
                  token,
                  http_event_server,
                  cw_logger,
                  number_of_threads,
                  maxByteLength,
                  debug,
                  timeout=60.0
                ):

        self.token             = token
        self.cw_logger         = cw_logger
        self.timeout           = timeout
        self.debug             = debug
        self.number_of_threads = number_of_threads
        self.server_uri        = http_event_server
        self.maxByteLength     = maxByteLength
        self.input_type        = 'json'
        self.headers           = {'Authorization':'Splunk '+self.token}

        if self.debug:
            self.cw_logger.debug('Token is:'+self.token)
            self.cw_logger.debug('URI is:'+self.server_uri)
            self.cw_logger.debug('Input Type:'+self.input_type)

        requests.packages.urllib3.disable_warnings()
        # build the Queue and the threads
        self._buildThreads()


    def _batchThread(self):
        while True:
            item = self.flushQueue.get()
            if item is None:
                break
            else:
                payload = ' '.join(item)
                if self.debug:
                    self.cw_logger.debug('Thread Called:'+threading.currentThread().name+'. Getting from Queue')
                    self.cw_logger.debug('Events received on thread:'+threading.currentThread().name+'. Sending to Splunk.')
                self._sendToSplunk(payload)
                self.flushQueue.task_done()


    def _sendToSplunk(self,payload):
         try:
            r = requests.post(self.server_uri, data=payload, headers=self.headers, verify=False, timeout=self.timeout)
         except ( requests.exceptions.Timeout,
                  requests.exceptions.ConnectionError,
                  requests.exceptions.RequestException )  as error:
                # Send back Timeout
                # Bad connectivity:DNS, Network
                # General catch for errors
                with error_lock:
                    self.cw_logger.error('CONNECTION ERROR:'+str(error))
                    self.error[Constants.COUNT] += 1
                    self.error[Constants.MSG] = {Constants.REASON : str(error) }
                return
         else:
             # to see if we got a bad return code we first raise that error
             # then capture it in the except
             try:
                 r.raise_for_status()
             except requests.exceptions.HTTPError  as error:
                 # General catch for any non 200 return
                 self.cw_logger.error('HTTP ERROR:'+str(error))
                 with error_lock:
                     self.error[Constants.COUNT] += 1
                     self.error[Constants.MSG] = json.loads(r.text)
                     self.error[Constants.MSG][Constants.REASON] = str(error)
                 return
             else:
                 # Everything is fine, send to cw if in debug mode
                 if self.debug:
                    self.cw_logger.error('HTTP Error Code:'+str(r.status_code)+' TEXT:'+r.text)
                 return

    def kill(self):
        del self

    def _buildThreads(self):
        # set up the Queue
        self.flushQueue = Queue.Queue(0)

        # keep count of how many we build
        threadcount = 0
        retries     = 3
        # Try to build the requested number of threads
        if self.debug:
            self.cw_logger.debug('Requested Threads:'+str(self.number_of_threads))
            self.cw_logger.debug('Total threads on container is:'+str(len(threading.enumerate())))
        for x in range(self.number_of_threads):
            for y in range(retries):
                t = threading.Thread(target=self._batchThread)
                t.daemon = True
                try:
                    t.start()
                except:
                    # if we have issues, will try each thread retries times
                    if( y < retries):
                        if self.debug:
                            self.cw_logger.warning('Warning Cant Start Thread Number:'+str(x)+' Will Retry:'+str(y))
                        # sleeping 2 seconds, since other lambdas should finish
                        sleep(2)
                        continue
                    else:
                        # 3 times for each thread
                        break
                else:
                    threadcount += 1
                    break

        if( threadcount > 0):
            if self.debug:
                self.cw_logger.debug('Started:'+str(threadcount)+' threads out of the:'+str(self.number_of_threads)+' requested.')
                self.cw_logger.debug('NOW the total threads on container is:'+str(len(threading.enumerate())))
            self.error = {Constants.COUNT: 0 }
            self.error[Constants.MSG] = {Constants.REASON : '' }
        else:
            self.error = {Constants.COUNT: 1 }
            self.error[Constants.MSG] = {Constants.REASON : 'Cant Start Any Threads' }


    def _waitUntilDone(self):
        # make sure all threads are done
        self.flushQueue.join()
        # send signal to kill the queues
        for i in range(self.number_of_threads):
            self.flushQueue.put(None)
        return

    def _sendEvent(self, event):
        self.flushQueue.put(event)
