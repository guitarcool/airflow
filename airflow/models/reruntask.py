import os
from enum import Enum
from datetime import datetime
import pytz
from jinja2 import Environment, PackageLoader
from pendulum import pendulum
from sqlalchemy import (
    Column, Integer, String, Boolean, PickleType, Index, UniqueConstraint, func, DateTime
)

from airflow import LoggingMixin, settings, models
from airflow.models import Base, ID_LEN, DagRun, DagBag, DagModel
from airflow.utils.db import provide_session


class ReRunTask(Base, LoggingMixin):
    """
    ReRunTask store the information of a re-run task .
    """

    __tablename__ = "rerun_task"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    etl_task_id = Column(String(250))
    rerun_start_date = Column(String(20))
    rerun_end_date = Column(String(20))
    _rerun_downstreams = Column('rerun_downstreams', String(1000))

    DAG_PREFIX = 'rerun__'
    TEMPLATE_PACKAGE = 'airflow.dags'
    TEMPLATE_FILE_NAME = 'rerun_dag.j2'
    Status = {
        '未执行': 0,
        '正在执行': 1,
        '执行成功': 2,
        '执行失败': 3
    }

    def __init__(self, task_id, dag_id, etl_task_id, rerun_start_date, rerun_end_date, rerun_downstreams):
        self.task_id = task_id
        self.dag_id = dag_id
        self.etl_task_id = etl_task_id
        self.rerun_start_date = rerun_start_date
        self.rerun_end_date = rerun_end_date
        self.rerun_downstreams = rerun_downstreams

    def update(self, etl_task_id, rerun_start_date, rerun_end_date, rerun_downstreams):
        self.etl_task_id = etl_task_id
        self.rerun_start_date = rerun_start_date
        self.rerun_end_date = rerun_end_date
        self.rerun_downstreams = rerun_downstreams

    @property
    def rerun_dag_id(self):
        return ReRunTask.DAG_PREFIX + self.dag_id + '__' + self.task_id

    @property
    def rerun_dag_file_path(self):
        rerun_dag_file_name = self.rerun_dag_id + '.py'
        return os.path.join(settings.DAGS_FOLDER, rerun_dag_file_name)

    @property
    def rerun_downstreams(self):
        if not self._rerun_downstreams:
            return []
        return [i.strip() for i in self._rerun_downstreams.split(',')]

    @rerun_downstreams.setter
    def rerun_downstreams(self, downstream_list):
        self._rerun_downstreams = ','.join(downstream_list) if downstream_list else ''

    def create_or_update_rerun_dag(self):
        """
        根据rerun_dag.j2模版在AIRFLOW_HOME目录下生成用于重跑的dag文件
        :return:
        """
        # 创建DAG python文件
        env = Environment(loader=PackageLoader(ReRunTask.TEMPLATE_PACKAGE))
        template = env.get_template(ReRunTask.TEMPLATE_FILE_NAME)
        rerun_task_ids = [self.etl_task_id]
        rerun_task_ids.extend(self.rerun_downstreams)
        content = template.render(dag_id=self.rerun_dag_id, base_dag_id=self.dag_id,
                                  start_date=self.rerun_start_date, end_date=self.rerun_end_date,
                                  rerun_task_ids=rerun_task_ids.__str__())
        with open(self.rerun_dag_file_path, 'w') as fp:
            fp.write(content)

        # 扫描dags文件夹，将扫描到的重跑dag存入数据库，使得创建的用于重跑的dag可以立即访问
        dagbag = DagBag(settings.DAGS_FOLDER)
        dag = dagbag.get_dag(self.rerun_dag_id)
        dag.sync_to_db()

    def delete_rerun_dag(self):
        # 1.删除生成的dag文件
        self.log.info('删除%s文件' % self.rerun_dag_file_path)
        os.remove(self.rerun_dag_file_path)
        # 2.调用dag删除接口
        from airflow.api.common.experimental import delete_dag
        delete_dag.delete_dag(self.rerun_dag_id)

    @property
    def rerun_status(self):
        status = '未执行'
        # 如调度开关打开，则属于执行状态
        orm_dag = DagModel.get_dagmodel(self.rerun_dag_id)
        if not orm_dag.is_paused:
            status = '正在执行'

        start_date = datetime.strptime(self.rerun_start_date, '%Y-%m-%d')
        end_date = datetime.strptime(self.rerun_end_date, '%Y-%m-%d')
        days = (end_date - start_date).days + 1
        dagruns = DagRun.find(dag_id=self.rerun_dag_id)

        success_dagruns = []
        failed_dagruns = []
        for dagrun in dagruns:
            if dagrun.get_state() == 'success':
                success_dagruns.append(dagrun)
            if dagrun.get_state() == 'failed':
                failed_dagruns.append(dagrun)
        # 如dag运行成功的实例数与重跑天数一致，则为执行成功状态
        if success_dagruns.__len__() == days:
            status = '执行成功'
        # 存在失败的dag运行成功的实例，则为执行失败状态
        if failed_dagruns:
            status = '执行失败'
        return status

    def run(self):
        DagModel.get_dagmodel(self.rerun_dag_id).set_is_paused(is_paused=False)

    # @provide_session
    # def set_state(self, state, session=None, commit=True):
    #     if state == RerunState.Running.value:
    #         self.try_number = self.try_number + 1
    #     self.rerun_state = state
    #     session.merge(self)
    #     if commit:
    #         session.commit()
    #
    # def exec(self):
    #     if self.task_type == ETLTaskType.ScheduledTask.value:
    #         return
    #     self.set_state(RerunState.Running.value)
    #     self._log = logging.getLogger("airflow.etltask")
    #     self._set_context(self)
    #     t = threading.Thread(target=self.exec_rerun_task, name='exec_rerun_task')
    #     t.start()
    #     print('完毕')
    #
    # def exec_rerun_task(self):
    #     try:
    #         self.log.info('start to execute task: %s', self.task_id)
    #         date = self.rerun_start_date
    #         while datetime.strptime(date, '%Y-%m-%d') <= datetime.strptime(self.rerun_end_date, '%Y-%m-%d'):
    #             # TODO 调用etl的下载加载程序
    #             time.sleep(2)
    #             self.log.info('finish the etl process with date %s', date)
    #             date = add_date(date)
    #     except Exception as e:
    #         self.log.error('execute task failed:', exc_info=True)
    #         self.set_state(RerunState.Failed.value)
    #     self.log.info('finish to execute task: %s', self.task_id)
    #     self.set_state(RerunState.Succeed.value)
    #
    # def get_log_filepath(self, try_number):
    #     log = os.path.expanduser(configuration.conf.get('core', 'BASE_LOG_FOLDER'))
    #     return ("{log}/{dag_id}/{task_id}/{try_number}.log".format(
    #         log=log, dag_id=self.dag_id, task_id=self.task_id, try_number=try_number))
    #
    # @property
    # def log_url(self):
    #     base_url = configuration.conf.get('webserver', 'BASE_URL')
    #     return base_url + (
    #         "/etl_log?"
    #         "task_id={task_id}"
    #         "&dag_id={dag_id}"
    #     ).format(task_id=self.task_id, dag_id=self.dag_id)
