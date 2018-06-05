"""
Description: Utility Class to handle configuration parameters
"""

import configparser
import sys
import ast


class ConfigUtil:
    SECTION_REQUIRED = 'REQUIRED PARAMETERS'
    SECTION_OPTIONAL = 'OPTIONAL PARAMETERS'

    KEY_MINS_TO_PROCESS     = 'minutes_to_process'
    KEY_BATCH_THREAD_SIZE   = 'batch_thread_size'
    KEY_RETRY_SLEEP_TIME    = 'retry_sleep_time'
    KEY_LOG_DESTINATION     = 'log_destination'
    KEY_LOG_MSG_PREFIX      = 'log_message_prefix'
    KEY_DEBUG               = 'debug'

    KEY_S3_BUCKET           = 'aws_s3_bucket_name'
    KEY_SOURCE_TYPE         = 'splunk_source_type'
    KEY_S3_BUCKET_PREFIX    = 'aws_s3_bucket_prefix'
    KEY_SPLUNK_HEC_KEY      = 'splunk_hec_key'
    KEY_SPLUNK_HEC_ENDPOINT = 'splunk_hec_endpoint'
    KEY_SPLUNK_DEBUG_SOURCETYPE = 'splunk_debug_sourcetype'

    KEY_VALUE               = 'value'
    KEY_TYPE                = 'type'

    # Required configuration parameters
    REQUIRED_PARAMETERS_LIST = [KEY_S3_BUCKET, KEY_SOURCE_TYPE, KEY_S3_BUCKET_PREFIX, KEY_SPLUNK_HEC_KEY, KEY_SPLUNK_HEC_ENDPOINT]

    # Optional configuration parameters
    OPTIONAL_PARAMETERS_DICT = {KEY_MINS_TO_PROCESS: {KEY_VALUE:15, KEY_TYPE:'int'}, KEY_BATCH_THREAD_SIZE: {KEY_VALUE:5, KEY_TYPE:'int'}
        , KEY_RETRY_SLEEP_TIME: {KEY_VALUE:10, KEY_TYPE:'int'}
        , KEY_LOG_DESTINATION: {KEY_VALUE:'CLOUDWATCH', KEY_TYPE:'string'}
        , KEY_LOG_MSG_PREFIX: {KEY_VALUE:'Time_ms=%d RequestId=%s Severity=%s Msg:%s', KEY_TYPE:'string'}
        , KEY_DEBUG: {KEY_VALUE:False, KEY_TYPE:'boolean'}
        , KEY_SPLUNK_DEBUG_SOURCETYPE: {KEY_VALUE:'splunk:Lambda', KEY_TYPE:'string'}}

    # Master configuration dictionary
    config = dict()
    is_valid_config = True

    # Default constructor
    def __init__(self):
        self.config = dict()

    # Load configuration parameters from the 'file'
    def load(self, file):
        try:
            config_parser = configparser.ConfigParser()
            config_parser.read_file(file)
            self.config = config_parser.__dict__['_sections']
        except configparser.Error as error:
            print('[ERROR] Process terminated: {} '.format(error))
            sys.exit(1)

    def set(self, section, key, value):
        self.config[section][key] = value

    def set_required(self, key, value):
        self.set(self.SECTION_REQUIRED, key, value)

    def set_optional(self, key, value):
        self.set(self.SECTION_OPTIONAL, key, value)

    def add_optional_section(self):
        default_dict = {}
        for key,value_dict in self.OPTIONAL_PARAMETERS_DICT.items():
            default_dict[key]=value_dict['value']
        self.config[self.SECTION_OPTIONAL] = default_dict

    def get(self, section, key):
        val = ''
        try:
            val = self.config[section][key]
        except KeyError:
            val = ''
        return val

    def get_config_value(self,key):
        return self.config[key]

    # Get configuration value for the key if present.
    # Return empty if otherwise
    def get_required_config(self, key):
        return self.get(self.SECTION_REQUIRED, key)

    # Get configuration value for the key if present.
    # Return empty if otherwise
    def get_optional_config(self, key):
        return self.get(self.SECTION_OPTIONAL, key)

    # Returns True if specified key is present in the SECTION_REQUIRED section
    # Returns False if otherwise
    def is_required_key_exists(self, key):
        return self.is_key_exists(self.SECTION_REQUIRED, key)

    # Returns True if specified key is present in the SECTION_OPTIONAL section
    # Returns False if otherwise
    def is_optional_key_exists(self, key):
        return self.is_key_exists(self.SECTION_OPTIONAL, key)

    # Returns True if specified 'key' exists for the 'section' in the master configuration.
    # Returns False otherwise.
    def is_key_exists(self, section, key):
        is_exists = False

        if self.is_section(section):
            section_dict = self.config[section]

            is_exists = key in section_dict.keys()
        else:
            is_exists = False

        return is_exists

    # Returns True if 'section' is present in the master configuration.
    # Returns False otherwise
    def is_section(self, section):
        return section in self.config.keys()

    def change_data_type(self):
        for key,value in self.config[self.SECTION_OPTIONAL].items():
            if key in self.OPTIONAL_PARAMETERS_DICT:

                try:
                    if self.OPTIONAL_PARAMETERS_DICT[key][self.KEY_TYPE] == 'int':
                        self.config[self.SECTION_OPTIONAL][key] = int(value)

                    elif self.OPTIONAL_PARAMETERS_DICT[key][self.KEY_TYPE] == 'boolean':
                        self.config[self.SECTION_OPTIONAL][key] = ast.literal_eval(str(value))

                except ValueError as e:

                    self.set_optional(key, self.OPTIONAL_PARAMETERS_DICT[key][self.KEY_VALUE])
                    print('[WARNING] Value Error: Invalid optional param value [key,value] [{},{}]. Value has been set to default {}'.format(key,value,self.OPTIONAL_PARAMETERS_DICT[key]))

    def merge_dict(self):
        final_dict = dict(self.config[self.SECTION_REQUIRED].items() | self.config[self.SECTION_OPTIONAL].items())
        self.config = final_dict

    def complete_init(self):
        self.change_data_type()
        self.merge_dict()

    def set_valid_status(self, status):
        self.is_valid_config = status

    def is_valid(self):
        return self.is_valid_config
