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
import importlib
import os
from datetime import timedelta
from datetime import datetime
from enum import Enum

from airflow.utils import timezone
from sqlalchemy import (
    Column, Integer, String, Boolean, PickleType, Index, UniqueConstraint, func, DateTime, or_,
    and_
)

from airflow import settings, models
from airflow.models.base import Base, ID_LEN
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.connection import Connection
# from etl.ods import zjrcb_ftp_loader
# from etl.dds import control

from airflow.utils.trigger_rule import TriggerRule


def add_date(date_str):
    date = datetime.strptime(date_str, '%Y-%m-%d') + timedelta(days=1)
    return date.strftime('%Y-%m-%d')


class ETLTaskType(Enum):
    DownloadTask = 0
    LoadDDSTask = 1
    ApplicationTask = 2


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
    dependent_tables = Column(String(1000))
    python_module_name = Column(String(100))
    _dependencies = Column('dependencies', String(1000))

    __table_args__ = (
        Index('ti_period', period_type, period_hour),
    )

    def __init__(self, task_id, dag_id, task_type, conn_id, sys_id, src_path, dst_path, flag_to_download,
                 time_to_download, period_type, period_weekday, dependent_tables, python_module_name, dependencies):
        self.task_id = task_id.strip()
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
        # self.period_hour = period_hour
        self.dependent_tables = dependent_tables
        self.python_module_name = python_module_name
        self.dependencies = dependencies

    def update(self, task_type, conn_id, sys_id, src_path, dst_path, flag_to_download, time_to_download, period_type,
               period_weekday, dependent_tables, python_module_name, dependencies):
        self.task_type = task_type
        self.conn_id = conn_id
        self.sys_id = sys_id
        self.src_path = src_path
        self.dst_path = dst_path
        self.flag_to_download = flag_to_download
        self.time_to_download = time_to_download
        self.period_type = period_type
        self.period_weekday = period_weekday
        # self.period_hour = period_hour
        self.dependent_tables = dependent_tables
        self.python_module_name = python_module_name
        self.dependencies = dependencies

    @property
    def _log(self):
        return logging.getLogger("airflow.task")

    @property
    def dependencies(self):
        if not self._dependencies:
            return []
        return [i.strip() for i in self._dependencies.split(',')]

    @dependencies.setter
    def dependencies(self, dependencies_list):
        self._dependencies = ','.join(dependencies_list) if dependencies_list else ''

    def get_deps_selections(self):
        """
        获取某个任务的可选前置依赖项
        根据任务类型获取可选依赖并过滤任务的后置依赖项及任务本身，从而避免出现循环依赖的情况
        :return:
        """

        deps_selects = ETLTask.get_deps_selects_for_types(self.dag_id)[self.task_type]
        downstream_ids = self.get_downstream_task_ids()
        return list(filter(lambda x: x not in downstream_ids and x != self.task_id, deps_selects))

    def get_dag_task(self):
        """
        获取对应的DAG中定义的任务对象
        :return:
        """
        dagbag = models.DagBag(settings.DAGS_FOLDER)
        dag = dagbag.get_dag(self.dag_id)
        task = dag.get_task(self.task_id)
        return task

    def get_downstream_task_ids(self):
        """
        获取任务的所有后置依赖项ID
        :return:
        """
        dag_task = self.get_dag_task()
        return dag_task.get_flat_relative_ids(upstream=False)

    def get_dependent_tbls_list(self):
        """
        获取直接依赖的所有表
        :return: List
        """
        if not self.dependent_tables:
            return []
        return [i.strip() for i in self.dependent_tables.split(',')]

    @provide_session
    def get_connection(self, session):
        conn = session.query(Connection).filter(
            Connection.id == self.conn_id,
        ).first()
        return conn

    @staticmethod
    def get_dds_task_ids(dag_id):
        return ETLTask.get_task_ids(dag_id=dag_id, task_type=ETLTaskType.LoadDDSTask.value)

    @staticmethod
    def get_download_task_ids(dag_id):
        return ETLTask.get_task_ids(dag_id=dag_id, task_type=ETLTaskType.DownloadTask.value)

    @staticmethod
    def get_apply_task_ids(dag_id):
        return ETLTask.get_task_ids(dag_id=dag_id, task_type=ETLTaskType.ApplicationTask.value)

    @staticmethod
    @provide_session
    def get_task_ids(session, dag_id, task_type):
        return [task_id for (task_id,)
                in session.query(ETLTask.task_id).filter(
                ETLTask.dag_id == dag_id,
                ETLTask.task_type == task_type,
            ).all()]

    @staticmethod
    def get_deps_selects_for_types(dag_id):
        deps_selects = {ETLTaskType.DownloadTask.value: [],
                        ETLTaskType.LoadDDSTask.value: ETLTask.get_download_task_ids(dag_id),
                        ETLTaskType.ApplicationTask.value: ETLTask.get_dds_task_ids(
                            dag_id) + ETLTask.get_apply_task_ids(dag_id)}
        return deps_selects

    def depent_on_dds_task(self):
        """
        是否依赖DDS加载任务
        :return: bool
        """
        return set(self.dependencies) & set(ETLTask.get_dds_task_ids(self.dag_id))

    def depent_on_dds_tbls(self):
        return self.task_type == ETLTaskType.ApplicationTask.value and self.depent_on_dds_task() and self.dependent_tables

    def get_trigger_rule(self):
        """
        根据任务不同类型获取不同的触发规则：
        如果任务为应用任务且依赖于DDS任务并指定了依赖的表，则触发规则为TriggerRule.ALL_DONE(上游任务执行完毕，无论成功或失败)
        否则 为TriggerRule.ALL_SUCCESS
        :return: trigger_rule
        """
        if self.depent_on_dds_tbls():
            return TriggerRule.ALL_DONE
        else:
            return TriggerRule.ALL_SUCCESS

    def execute(self, etl_date, ti):
        """
        根据任务的类型调用不同的etl程序
        :param etl_date: etl 日期
        :return:
        """
        self._log.info('etl_date:' + etl_date)
        if self.task_type == ETLTaskType.DownloadTask.value:
            result = self._exec_download(etl_date, ti)
        elif self.task_type == ETLTaskType.LoadDDSTask.value:
            result = self._exec_load_dds(etl_date, ti)
        else:
            result = self._exec_apply_task(etl_date, ti)
        return result

    def _exec_download(self, etl_date, ti):
        self._log.info('下载任务开始执行')
        if self.period_type == PeriodType.Weekly.value and timezone.now().weekday() + 1 != self.period_weekday:
            self._log.info('未到执行周期，本次任务不执行')
            return 'skiped'
        # conn = self.get_connection()
        # result = zjrcb_ftp_loader.run_ods(system=self.sys_id, src_path=self.src_path, dst_path=self.dst_path,
        #                          check_mode=self.flag_to_download, tbls_ignored_errors=self.tbls_ignored_errors,
        #                          ftp_host=conn.host, ftp_port=conn.port, ftp_username=conn.login,
        #                         ftp_password=conn.get_password(), etl_date=etl_date)
        result = {'success': ['t',  't1', 't2', 't3', 't4', 't5', 't6', 't7','t8', 't9', 't10', 't11', 't12'],
                  'failed': [],
                  'unprocessed': ['tb1']
                  }
        if result['failed']:
            ti.xcom_push(key=XCOM_RETURN_KEY, value=result)
            raise Exception('tables %s load ods failed :' % result['failed'])
        else:
            return result

    def _exec_load_dds(self, etl_date, ti):
        self._log.info('DDS加载任务开始执行')
        # result = control.load_dds_sys(self.sys_id, etl_date)
        result = {'success': ['t',  't1', 't2', 't3', 't4', 't5', 't6', 't7','t8', 't9', 't10', 't11', 't12'],
                  'failed': ['tbl_name5', 'tbl_name6', 'tbl_name7', 'tbl_name8'],
                  'unprocessed': ['tb1']
                  }
        if result['failed']:
            ti.xcom_push(key=XCOM_RETURN_KEY, value=result)
            raise Exception('tables %s load ods failed :' % result['failed'])
        else:
            return result

    def _exec_apply_task(self, etl_date, ti):
        """
        执行应用类任务, 分两种情况：
            1.应用任务依赖DDS任务（指定了依赖表），触发规则为ALL_DONE，需先根据DDS任务结果判断表依赖条件是否满足
            2.应用任务不依赖DDS任务，触发规则为ALL_SUCCESS，直接执行相应脚本
        :param etl_date: ETL日期
        :param deps_results: 依赖任务的执行结果
        :return:
        """

        self._log.info('应用任务开始执行')
        if self.depent_on_dds_tbls():
            # 1.获取依赖任务的所有返回结果，
            deps_results = ti.xcom_pull(task_ids=self.dependencies)
            # 2.若结果对象数目与依赖任务数不一致，则说明存在某个依赖的任务执行过程出现异常，当前任务需终止。
            if len(deps_results) != len(self.dependencies):
                raise Exception('dependent task execution failed')
            # 3.若任务依赖的表存在于返回结果执行失败的表中，当前任务需终止。
            failed_tbls = []
            for result in deps_results:
                if type(result) == dict and result['failed']:
                    failed_tbls.extend(result['failed'])
            if set(failed_tbls) & set(self.get_dependent_tbls_list()):
                raise Exception('dependent table execution failed')

        # module_name = 'etl.udm.' + os.path.splitext(self.python_module_name)[0]
        # udm = importlib.import_module(module_name)
        # udm.run(etl_date)
        return 'success'
