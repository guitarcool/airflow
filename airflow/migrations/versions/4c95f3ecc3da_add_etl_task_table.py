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

"""add etl task table

Revision ID: 4c95f3ecc3da
Revises: 965f2935e326
Create Date: 2019-07-15 14:04:22.576924

"""

# revision identifiers, used by Alembic.
revision = '4c95f3ecc3da'
down_revision = '965f2935e326'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table('etl_task',
                    sa.Column('task_id', sa.String(length=250), nullable=False),
                    sa.Column('dag_id', sa.String(length=250), nullable=False),
                    sa.Column('task_type', sa.Integer, nullable=False),
                    sa.Column('src_path', sa.String(length=100), nullable=True),
                    sa.Column('file_pattern', sa.String(length=100), nullable=True),
                    sa.Column('dst_path', sa.String(length=100), nullable=True),
                    sa.Column('dst_tbl', sa.String(length=50), nullable=True),
                    sa.Column('conn_id', sa.Integer, nullable=True),
                    sa.Column('check_mode', sa.Integer, nullable=True),
                    sa.Column('check_mode_remk', sa.String(length=50), nullable=True),
                    sa.Column('period_type', sa.Integer, nullable=True),
                    sa.Column('period_weekday', sa.Integer, nullable=True),
                    sa.Column('period_hour', sa.Integer, nullable=True),
                    sa.Column('exec_logic_type', sa.Integer, nullable=True),
                    sa.Column('exec_logic_preset_type', sa.Integer, nullable=True),
                    sa.Column('exec_logic_custom_sql', sa.String(length=1000), nullable=True),
                    sa.Column('error_handle', sa.Integer, nullable=True),
                    sa.Column('rerun_start_date', sa.String(length=20), nullable=True),
                    sa.Column('rerun_end_date', sa.String(length=20), nullable=True),
                    sa.Column('rerun_state', sa.Integer, nullable=True, default=0),
                    sa.Column('try_number',  sa.Integer, nullable=True, default=0),
                    sa.PrimaryKeyConstraint('task_id', 'dag_id'))


def downgrade():
    op.drop_table('etl_task')
