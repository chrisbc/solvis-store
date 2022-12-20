"""
This module exports comfiguration forthe current system
and is imported  by the various run_xxx.py scripts
"""

import enum
import os


class EnvMode(enum.IntEnum):
    AWS = 0
    LOCAL = 1


def boolean_env(environ_name, default='False'):
    return bool(os.getenv(environ_name, default).upper() in ["1", "Y", "YES", "TRUE"])


IS_TESTING = boolean_env('TESTING', 'False')
IS_OFFLINE = boolean_env('SLS_OFFLINE')  # set by serverless-wsgi plugin

REGION = os.getenv('REGION', 'us-east-1')
DEPLOYMENT_STAGE = os.getenv('DEPLOYMENT_STAGE', 'LOCAL').upper()
LOGGING_CFG = os.getenv('LOGGING_CFG', 'api/logging.yaml')
CLOUDWATCH_APP_NAME = os.getenv('CLOUDWATCH_APP_NAME', 'CLOUDWATCH_APP_NAME_unconfigured')
