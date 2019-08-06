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

"""add download_config table to db

Revision ID: f401993f1c2d
Revises: 93f489151926
Create Date: 2019-08-05 12:03:06.115440

"""

# revision identifiers, used by Alembic.
revision = 'f401993f1c2d'
down_revision = '93f489151926'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa



def upgrade():
    op.create_table('download_config',
                    sa.Column('id', sa.Integer(), nullable=False),
                    sa.Column('sys_id', sa.String(length=250), nullable=True),
                    sa.Column('conn_id', sa.Integer, nullable=True),
                    sa.Column('src_path', sa.String(length=200), nullable=True),
                    sa.Column('dst_path', sa.String(length=200), nullable=True),
                    sa.Column('flag_to_download', sa.String(length=50), nullable=True),
                    sa.Column('time_to_download', sa.String(length=50), nullable=True),
                    sa.PrimaryKeyConstraint('id'),
                    sa.ForeignKeyConstraint(['conn_id'],
                                            ['connection.id'], ),
                    sa.UniqueConstraint('sys_id'))


def downgrade():
    op.drop_table('download_config')

