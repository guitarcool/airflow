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

"""add rerun task table to db

Revision ID: 93f489151926
Revises: 4c95f3ecc3da
Create Date: 2019-07-30 17:06:55.239528

"""

# revision identifiers, used by Alembic.
revision = '93f489151926'
down_revision = '4c95f3ecc3da'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table('rerun_task',
                    sa.Column('task_id', sa.String(length=250), nullable=False),
                    sa.Column('dag_id', sa.String(length=250), nullable=False),
                    sa.Column('etl_task_id', sa.String(length=250)),
                    sa.Column('rerun_start_date', sa.String(length=20), nullable=True),
                    sa.Column('rerun_end_date', sa.String(length=20), nullable=True),
                    sa.Column('_rerun_downstreams', sa.String(length=1000), nullable=True),
                    sa.PrimaryKeyConstraint('task_id', 'dag_id'))


def downgrade():
    op.drop_table('rerun_task')
