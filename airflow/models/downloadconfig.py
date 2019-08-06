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
    and_,
    ForeignKey)
from sqlalchemy.orm import relationship

from airflow.models.base import Base, ID_LEN
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.connection import Connection


class FlagToDownload(Enum):
    OKFlag = 0
    FileSize = 1
    AfterFixedTime = 2


class DownloadConfig(Base, LoggingMixin):
    """
     store information about download config of a system
    """

    __tablename__ = "download_config"

    id = Column(Integer, primary_key=True)
    sys_id = Column(String(ID_LEN), unique=True)
    conn_id = Column(Integer(), ForeignKey('connection.id'),)
    src_path = Column(String(200))
    dst_path = Column(String(200))
    flag_to_download = Column(String(50))
    time_to_download = Column(String(50))
    connection = relationship(
        "Connection",
        cascade=False,
        cascade_backrefs=False, backref='download_configs')

    def __init__(self, sys_id=None, conn_id=None, src_path=None, dst_path=None, flag_to_download=None,
                 time_to_download=None):
        self.conn_id = conn_id
        self.sys_id = sys_id
        self.src_path = src_path
        self.dst_path = dst_path
        self.flag_to_download = flag_to_download
        self.time_to_download = time_to_download

    _download_flag_types = [
        ('ok_flag', '标志文件',),
        ('file_size', '文件大小不变'),
        ('after_fixed_time', '固定时间之后'),
    ]
