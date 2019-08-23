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
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from airflow.utils import timezone
from flask_admin.form import DateTimePickerWidget
from wtforms import Field, DateTimeField, SelectField, StringField
from flask_admin.form import DateTimePickerWidget, DatePickerWidget
from wtforms import DateTimeField, SelectField, DateField

from flask_wtf import FlaskForm


class DateTimeForm(FlaskForm):
    # Date filter form for task views
    execution_date = DateTimeField(
        "Execution date", widget=DateTimePickerWidget())


class DateTimeWithNumRunsForm(FlaskForm):
    # Date time and number of runs form for tree view, task duration
    # and landing times
    base_date = DateTimeField(
        "Anchor date", widget=DateTimePickerWidget(), default=timezone.utcnow())
    num_runs = SelectField("Number of runs", default=25, choices=(
        (5, "5"),
        (25, "25"),
        (50, "50"),
        (100, "100"),
        (365, "365"),
    ))


class DateTimeWithNumRunsWithDagRunsForm(DateTimeWithNumRunsForm):
    # Date time and number of runs and dag runs form for graph and gantt view
    execution_date = SelectField("DAG run")


class DatePeriodForm(FlaskForm):
    start_date = DateField(
        "Start date", widget=DatePickerWidget(), default=timezone.utcnow())
    end_date = DateField(
        "End date", widget=DatePickerWidget(), default=timezone.utcnow())


class DatePeriodWithTaskIdWithStateForm(DatePeriodForm):
    state = SelectField("State", default="", choices=(
        ('', '全部'),
        ('success', '成功'),
        ('failed', '失败'),
        ('running', '运行中'),
        ('queued', '排队中'),
        ('upstream_failed', '前置依赖失败'),
        ('no_status', '无状态'),
    ))
    task_id = StringField(label='task_id')

    def setStateChoices(choices):
        state = SelectField("state", default=choices[0][0], choices=choices)


class DispatchDateFormWithDagRunsForm(FlaskForm):
    # Date filter form for dag graph view
    dispatch_date = DateField(
        "Dispatch date", widget=DatePickerWidget(), default=timezone.utcnow())
    execution_date = SelectField("DAG run")

