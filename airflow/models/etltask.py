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
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import timedelta
from datetime import datetime
from enum import Enum
from sqlalchemy import (
    Column, Integer, String, Boolean, PickleType, Index, UniqueConstraint, func, DateTime, or_,
    and_
)
from airflow import configuration, settings
from airflow.models.base import Base, ID_LEN
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.connection import Connection



def add_date(date_str):
    date = datetime.strptime(date_str, '%Y-%m-%d') + timedelta(days=1)
    return date.strftime('%Y-%m-%d')


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
    rerun_state = Column(Integer(), default=0)
    try_number = Column(Integer(), default=0)

    __table_args__ = (
        Index('ti_period', period_type, period_hour),
        Index('ti_exec_logic', exec_logic_type, exec_logic_preset_type, exec_logic_custom_sql),
    )

    def __init__(self, task_id, dag_id, task_type, src_path, file_pattern, dst_path, dst_tbl, conn_id, check_mode, check_mode_remk,
                 period_type, period_weekday, period_hour, exec_logic_type, exec_logic_preset_type, exec_logic_custom_sql,
                 error_handle, rerun_start_date, rerun_end_date):
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
        self._log = logging.getLogger("airflow.etltask")

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

    @provide_session
    def set_state(self, state, session=None, commit=True):
        if state == RerunState.Running.value:
            self.try_number = self.try_number + 1
        self.rerun_state = state
        session.merge(self)
        if commit:
            session.commit()

    def exec(self):
        if self.task_type == ETLTaskType.ScheduledTask.value:
            return
        self.set_state(RerunState.Running.value)
        self._log = logging.getLogger("airflow.etltask")
        self._set_context(self)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(self.exec_rerun_task)

    def exec_rerun_task(self):
        try:
            self.log.info('start to execute task: %s', self.task_id)
            date = self.rerun_start_date
            while datetime.strptime(date, '%Y-%m-%d') <= datetime.strptime(self.rerun_end_date, '%Y-%m-%d'):
                # TODO 调用etl的下载加载程序
                self.log.info('finish the etl process with date %s', date)
                date = add_date(date)
        except Exception as e:
            self.log.error('execute task failed:', exc_info=True)
            self.set_state(RerunState.Failed.value)
        self.log.info('finish to execute task: %s', self.task_id)
        self.set_state(RerunState.Succeed.value)

    def get_log_filepath(self, try_number):
        log = os.path.expanduser(configuration.conf.get('core', 'BASE_LOG_FOLDER'))
        return ("{log}/{dag_id}/{task_id}/{try_number}.log".format(
            log=log, dag_id=self.dag_id, task_id=self.task_id, try_number=try_number))

    @property
    def log_url(self):
        base_url = configuration.conf.get('webserver', 'BASE_URL')
        return base_url + (
            "/etl_log?"
            "task_id={task_id}"
            "&dag_id={dag_id}"
        ).format(task_id=self.task_id, dag_id=self.dag_id)

