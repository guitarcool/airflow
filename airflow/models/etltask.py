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

import logging
from datetime import timedelta
from datetime import datetime
from enum import Enum
from sqlalchemy import (
    Column, Integer, String, Boolean, PickleType, Index, UniqueConstraint, func, DateTime, or_,
    and_
)
from airflow.models.base import Base, ID_LEN
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.connection import Connection


def add_date(date_str):
    date = datetime.strptime(date_str, '%Y-%m-%d') + timedelta(days=1)
    return date.strftime('%Y-%m-%d')


class ETLTaskType(Enum):
    DownloadTask = 0
    LoadDDSTask = 1
    UDMTask = 2
    ReportTask = 3


class FlagToDownload(Enum):
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


class ETLTask(Base, LoggingMixin):
    """
    ETLTask store the config information of a etl task .
    """

    __tablename__ = "etl_task"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    task_type = Column(Integer())
    conn_id = Column(Integer())
    sys_id = Column(String(20))
    src_path = Column(String(100))
    dst_path = Column(String(100))
    flag_to_download = Column(Integer())
    time_to_download = Column(String(50))
    period_type = Column(Integer())
    period_weekday = Column(Integer())
    period_hour = Column(Integer())
    tbls_ignored_errors = Column(String(1000))
    python_file_path = Column(String(100))
    _dependencies = Column('dependencies', String(1000))

    __table_args__ = (
        Index('ti_period', period_type, period_hour),
    )

    def __init__(self, task_id, dag_id, task_type, conn_id, sys_id, src_path, dst_path, flag_to_download, time_to_download,
                 period_type, period_weekday, period_hour, tbls_ignored_errors, python_file_path, dependencies):
        self.task_id = task_id
        self.dag_id = dag_id
        self.task_type = task_type
        self.conn_id = conn_id
        self.sys_id = sys_id
        self.src_path = src_path
        self.dst_path = dst_path
        self.flag_to_download = flag_to_download
        self.time_to_download = time_to_download
        self.period_type = period_type
        self.period_weekday = period_weekday
        self.period_hour = period_hour
        self.tbls_ignored_errors = tbls_ignored_errors
        self.python_file_path = python_file_path
        self.dependencies = dependencies
        self._log = logging.getLogger("airflow.etltask")

    def update(self, task_type, conn_id, sys_id, src_path, dst_path, flag_to_download, time_to_download, period_type,
               period_weekday, period_hour, tbls_ignored_errors, python_file_path, dependencies):
        self.task_type = task_type
        self.conn_id = conn_id
        self.sys_id = sys_id
        self.src_path = src_path
        self.dst_path = dst_path
        self.flag_to_download = flag_to_download
        self.time_to_download = time_to_download
        self.period_type = period_type
        self.period_weekday = period_weekday
        self.period_hour = period_hour
        self.tbls_ignored_errors = tbls_ignored_errors
        self.python_file_path = python_file_path
        self.dependencies = dependencies

    @property
    def dependencies(self):
        if not self._dependencies:
            return []
        return [i.strip() for i in self._dependencies.split(',')]

    @dependencies.setter
    def dependencies(self, dependencies_list):
        self._dependencies = ','.join(dependencies_list) if dependencies_list else ''

    def get_ignored_tbls(self):
        if not self.tbls_ignored_errors:
            return ''
        return [i.strip() for i in self.tbls_ignored_errors.split(',')]

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

    def execute(self):
        if self.task_type == ETLTaskType.DownloadTask.value:
            self._exec_download()
        elif self.task_type == ETLTaskType.LoadDDSTask.value:
            self._exec_load_dds()
        elif self.task_type == ETLTaskType.UDMTask.value:
            self._exec_udm()
        else:
            self._exec_report()

    def _exec_download(self):
        pass

    def _exec_load_dds(self):
        pass

    def _exec_udm(self):
        pass

    def _exec_report(self):
        pass
