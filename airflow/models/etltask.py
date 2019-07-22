# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import copy
import functools
import getpass
import hashlib
import logging
import math
import os
import signal
import time
from datetime import timedelta
from enum import Enum
from typing import Optional
from urllib.parse import quote
import lazy_object_proxy
import pendulum

import dill
from sqlalchemy import (
    Column, Integer, String, Boolean, PickleType, Index, UniqueConstraint, func, DateTime, or_,
    and_
)
from sqlalchemy.orm import reconstructor
from sqlalchemy.orm.session import Session

from airflow import configuration, settings
from airflow.exceptions import (
    AirflowException, AirflowTaskTimeout, AirflowSkipException, AirflowRescheduleException
)
from airflow.models.base import Base, ID_LEN
from airflow.models.log import Log
from airflow.models.pool import Pool
from airflow.models.taskfail import TaskFail
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.variable import Variable
from airflow.models.xcom import XCom, XCOM_RETURN_KEY
from airflow.settings import Stats
from airflow.ti_deps.dep_context import DepContext, QUEUE_DEPS, RUN_DEPS
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.email import send_email
from airflow.utils.helpers import is_container
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.models.connection import Connection
from airflow.utils.state import State
from airflow.utils.timeout import timeout


class ETLTaskType(Enum):
    ScheduledTask = 0
    RerunTask = 1


class CheckMode(Enum):
    OKFlag = 0
    FileSize = 1
    AfterFixedTime = 2


class Weekday(Enum):
    Sun = 0
    Mon = 1
    Tue = 2
    Wed = 3
    Thu = 4
    Fri = 5
    Sat = 6


class PeriodType(Enum):
    Today = 0
    Daily = 1
    Weekly = 2


class ExecLogicType(Enum):
    Preset = 0
    Custom = 1


class ExecLogicPresetType(Enum):
    Insert = 0
    InsertOverwrite = 1
    Update = 2
    Sqoop = 3


class ErrorHandle(Enum):
    Pause = 0
    Ignore = 1


class RerunState(Enum):
    NeverExecuted = 0
    Running = 1
    Succeed = 2
    Failed = 3


class ETLTask(Base, LoggingMixin):
    """
    ETLTask store the config information of a etl task .
    """

    __tablename__ = "etl_task"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    task_type = Column(Integer())
    src_path = Column(String(100))
    file_pattern = Column(String(100))
    dst_path = Column(String(100))
    dst_tbl = Column(String(50))
    conn_id = Column(Integer())
    check_mode = Column(Integer())
    check_mode_remk = Column(String(50))
    period_type = Column(Integer())
    period_weekday = Column(Integer())
    period_hour = Column(Integer())
    exec_logic_type = Column(Integer())
    exec_logic_preset_type = Column(Integer())
    exec_logic_custom_sql = Column(String(1000))
    error_handle = Column(Integer())
    rerun_start_date = Column(String(20))
    rerun_end_date = Column(String(20))
    rerun_state = Column(Integer())
    rerun_log_file_names = Column(String(200))

    __table_args__ = (
        Index('ti_period', period_type, period_hour),
        Index('ti_exec_logic', exec_logic_type, exec_logic_preset_type, exec_logic_custom_sql),
    )

    def __init__(self, task_id, dag_id, task_type, src_path, file_pattern, dst_path, dst_tbl, conn_id, check_mode, check_mode_remk,
                 period_type, period_weekday, period_hour, exec_logic_type, exec_logic_preset_type, exec_logic_custom_sql,
                 error_handle, rerun_start_date, rerun_end_date, rerun_state, rerun_log_file_names):
        self.task_id = task_id
        self.dag_id = dag_id
        self.task_type = task_type
        self.src_path = src_path
        self.file_pattern = file_pattern

        self.dst_path = dst_path
        self.dst_tbl = dst_tbl
        self.conn_id = conn_id
        self.check_mode = check_mode
        self.check_mode_remk = check_mode_remk
        self.period_type = period_type
        self.period_weekday = period_weekday
        self.period_hour = period_hour
        self.exec_logic_type = exec_logic_type
        self.exec_logic_preset_type = exec_logic_preset_type
        self.exec_logic_custom_sql = exec_logic_custom_sql
        self.error_handle = error_handle
        self.rerun_start_date = rerun_start_date
        self.rerun_end_date = rerun_end_date
        self.rerun_state = rerun_state
        self.rerun_log_file_names = rerun_log_file_names

    @provide_session
    def get_connection(self, session):
        """
        Returns the Connection of this Task

        :param session:
        :return: Connection
        """
        conn = session.query(Connection).filter(
            Connection.id == self.conn_id,
        ).first()
        return conn
