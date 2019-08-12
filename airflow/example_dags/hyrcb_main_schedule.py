import time
import airflow
import pendulum
import random
from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

DAG_NAME = 'hyrcb_main_schedule'
local_tz = pendulum.timezone("Asia/Shanghai")

dag_args = {
    'owner': 'hyrcb',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 10, tzinfo=local_tz),
    'end_date': datetime(2019, 5, 21, tzinfo=local_tz),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
}

main_dag = DAG(dag_id=DAG_NAME,default_args=dag_args,schedule_interval=timedelta(days=1),)

def print_msg(task, **kwargs):
    num = random.randint(1, 20)
    print(num)
    print('task_id:' + task.task_id)

pre_etl = PythonOperator(
    task_id='pre_etl',
    python_callable=print_msg,
    provide_context=True,
    dag=main_dag)

def ods_procedure(system_name, **kwargs):
    etl_date = kwargs['ds']
    # zjrcb_ftp_loader.run_ods(system_name, etl_date)
    print(etl_date)

def dds_procedure(sys_id, **kwargs):
    etl_date = kwargs['ds']
    # control.load_dds_sys(sys_id, etl_date)
    print(etl_date)

def udm_procedure(func, **kwargs):
    etl_date = kwargs['ds']
    # func(etl_date)
    print(etl_date)

def create_ods_pythonoperator(system_name):
    ods = PythonOperator(
			 task_id= system_name + '_ods',
			 python_callable=ods_procedure,
			 provide_context=True,
			 op_kwargs={'system_name': system_name},
			 dag=main_dag)
    return ods
							
def create_dds_pythonoperator(sys_id):
    dds= PythonOperator(
			task_id= sys_id + '_dds', 
			python_callable=dds_procedure,
			provide_context=True,
			op_kwargs={'sys_id': sys_id},
			dag=main_dag)
    return dds
	
def create_udm_pythonoperator(udm_name, run_fun):
   udm= PythonOperator(
		       task_id= udm_name + '_udm', 
		       python_callable=udm_procedure,
		       provide_context=True,
		       op_kwargs={'func': run_fun},
		       dag=main_dag)
   return udm
		
#ods/dds
core_ods = create_ods_pythonoperator('core')
core_dds = create_dds_pythonoperator('core')
nfcp_ods = create_ods_pythonoperator('nfcp')
nfcp_dds = create_dds_pythonoperator('nfcp')
ebills_ods = create_ods_pythonoperator('ebills')
ebills_dds = create_dds_pythonoperator('ebills')
gas_ods = create_ods_pythonoperator('gas')
gas_dds = create_dds_pythonoperator('gas')
xdzx_ods = create_ods_pythonoperator('xdzx')
xdzx_dds = create_dds_pythonoperator('xdzx')
imbs_ods = create_ods_pythonoperator('imbs')
imbs_dds = create_dds_pythonoperator('imbs')
ibs_ods = create_ods_pythonoperator('ibs')
ibs_dds = create_dds_pythonoperator('ibs')
ccrd_ods = create_ods_pythonoperator('ccrd')
ccrd_dds = create_dds_pythonoperator('ccrd')
fms_ods = create_ods_pythonoperator('fms')
fms_dds = create_dds_pythonoperator('fms')
ccpt_ods = create_ods_pythonoperator('ccpt')
ccpt_dds = create_dds_pythonoperator('ccpt')

#UDM
zh_ckzhb_udm = create_dds_pythonoperator('zh_ckzhb')
zh_ckjdfljb_udm = create_dds_pythonoperator('zh_ckjdfljb')
zh_dkzhzhb_udm = create_dds_pythonoperator('zh_dkzhzhb')
zh_dkjdfljb_udm = create_dds_pythonoperator('zh_dkjdfljb')
zh_ptdklxb_udm = create_dds_pythonoperator('zh_ptdklxb')
zh_ajdklxb_udm = create_dds_pythonoperator('zh_ajdklxb')
zh_jjkb_udm = create_dds_pythonoperator('zh_jjkb')
zh_xykzhb_udm = create_dds_pythonoperator('zh_xykzhb')
zh_xykzhzhb_udm = create_dds_pythonoperator('zh_xykzhzhb')
zh_xykzhjdfljb_udm = create_dds_pythonoperator('zh_xykzhjdfljb')
zh_lczhzhb_udm = create_dds_pythonoperator('zh_lczhzhb')
zh_nbzzhb_udm = create_dds_pythonoperator('zh_nbzzhb')
zh_nbzjdfljb_udm = create_dds_pythonoperator('zh_nbzjdfljb')
zh_ywqyzhb_udm = create_dds_pythonoperator('zh_ywqyzhb')
zh_fshlqyzhzhb_udm = create_dds_pythonoperator('zh_fshlqyzhzhb')

kh_khgbxxb_udm = create_dds_pythonoperator('kh_khgbxxb')
kh_dgkhjbxxb_udm = create_dds_pythonoperator('kh_dgkhjbxxb')
kh_dgkhkzxxb_udm = create_dds_pythonoperator('kh_dgkhkzxxb')
kh_grkhjbxxb_udm = create_dds_pythonoperator('kh_grkhjbxxb')
kh_khcyzhhzb_udm = create_dds_pythonoperator('kh_khcyzhhzb')
kh_gjywakhjslhzb_udm = create_dds_pythonoperator('kh_gjywakhjslhzb')
kh_gjywakhjljslhzb_udm = create_dds_pythonoperator('kh_gjywakhjljslhzb')
kh_khsxhzb_udm = create_dds_pythonoperator('kh_khsxhzb')

jy_kjjylshzb_udm = create_dds_pythonoperator('jy_kjjylshzb')
jy_ptdkjymxb_udm = create_dds_pythonoperator('jy_ptdkjymxb')
jy_ajdkhkmxb_udm = create_dds_pythonoperator('jy_ajdkhkmxb')
jy_hqckjyhzb_udm = create_dds_pythonoperator('jy_hqckjyhzb')
jy_lcjyhzb_udm = create_dds_pythonoperator('jy_lcjyhzb')
jy_fshljyhzb_udm = create_dds_pythonoperator('jy_fshljyhzb')
jy_ATMjyhzb_udm = create_dds_pythonoperator('jy_ATMjyhzb')
jy_POSjyhzb_udm = create_dds_pythonoperator('jy_POSjyhzb')
jy_POSbdtjyhzb_udm = create_dds_pythonoperator('jy_POSbdtjyhzb')
jy_qywyjyhzb_udm = create_dds_pythonoperator('jy_qywyjyhzb')
jy_grwyjyhzb_udm = create_dds_pythonoperator('jy_grwyjyhzb')

jg_jgckhzb_udm = create_dds_pythonoperator('jg_jgckhzb')
jg_jgdkhzb_udm = create_dds_pythonoperator('jg_jgdkhzb')
jg_jgjjkhzb_udm = create_dds_pythonoperator('jg_jgjjkhzb')
jg_jgxykhzb_udm = create_dds_pythonoperator('jg_jgxykhzb')
jg_jgxykzhhzb_udm = create_dds_pythonoperator('jg_jgxykzhhzb')
jg_jglchzb_udm = create_dds_pythonoperator('jg_jglchzb')
jg_gjywajgjslhzb_udm = create_dds_pythonoperator('jg_gjywajgjslhzb')
jg_jgnbzhzb_udm = create_dds_pythonoperator('jg_jgnbzhzb')

kj_kmdyb_udm = create_dds_pythonoperator('kj_kmdyb')
kj_kmyeb_udm = create_dds_pythonoperator('kj_kmyeb')
qt_llzhb_udm = create_dds_pythonoperator('qt_llzhb')

pre_etl >> core_ods >> core_dds
pre_etl >> xdzx_ods >> xdzx_dds
pre_etl >> nfcp_ods >> nfcp_dds 
pre_etl >> ebills_ods >> ebills_dds 
pre_etl >> ccrd_ods >> ccrd_dds 
pre_etl >> fms_ods >> fms_dds 
pre_etl >> ccpt_ods >> ccpt_dds 
pre_etl >> ibs_ods >> ibs_dds
pre_etl >> imbs_ods >> imbs_dds >> zh_ywqyzhb_udm 
pre_etl >> gas_ods >> gas_dds >> [kj_kmdyb_udm, kj_kmyeb_udm]


ccpt_dds >> [jy_ATMjyhzb_udm]
ibs_dds >> [jy_qywyjyhzb_udm, jy_grwyjyhzb_udm, kh_khgbxxb_udm, kh_grkhjbxxb_udm]
xdzx_dds >> [zh_dkzhzhb_udm, kh_khgbxxb_udm, kh_dgkhjbxxb_udm, kh_dgkhkzxxb_udm, kh_grkhjbxxb_udm, kh_khsxhzb_udm]
nfcp_dds >> [zh_fshlqyzhzhb_udm, kh_grkhjbxxb_udm, jy_fshljyhzb_udm]
fms_dds >> [zh_lczhzhb_udm, kh_khcyzhhzb_udm, jy_lcjyhzb_udm ]
ccrd_dds >> [zh_xykzhb_udm, zh_xykzhzhb_udm, zh_xykzhjdfljb_udm, kh_khgbxxb_udm, jy_ATMjyhzb_udm ]
ebills_dds >> [zh_ckzhb_udm, zh_dkzhzhb_udm, zh_nbzzhb_udm, kh_dgkhkzxxb_udm, kh_grkhjbxxb_udm, kh_gjywakhjslhzb_udm]
core_dds >> [zh_ckzhb_udm, zh_dkzhzhb_udm,  zh_jjkb_udm, zh_lczhzhb_udm, 
			zh_nbzzhb_udm, zh_nbzjdfljb_udm, zh_ywqyzhb_udm]
core_dds >> [kh_khgbxxb_udm, kh_dgkhjbxxb_udm, kh_dgkhkzxxb_udm, kh_grkhjbxxb_udm, kh_khcyzhhzb_udm]
core_dds >> [jy_ptdkjymxb_udm, jy_ATMjyhzb_udm]

zh_ckzhb_udm >> [zh_ckjdfljb_udm, jg_jgckhzb_udm]
zh_dkzhzhb_udm >> [zh_dkjdfljb_udm, jg_jgdkhzb_udm]
kh_khcyzhhzb_udm  >> jy_kjjylshzb_udm 
zh_jjkb_udm >> [zh_ptdklxb_udm, zh_ajdklxb_udm, jg_jgjjkhzb_udm ]
zh_xykzhb_udm >> jg_jgxykhzb_udm 
zh_xykzhzhb_udm >> jg_jgxykzhhzb_udm
zh_lczhzhb_udm >> jg_jglchzb_udm 
zh_nbzzhb_udm >> jg_jgnbzhzb_udm >> qt_llzhb_udm
kh_gjywakhjslhzb_udm >> [kh_gjywakhjljslhzb_udm, jg_gjywajgjslhzb_udm]
jy_ATMjyhzb_udm >> [jy_POSjyhzb_udm, jy_POSbdtjyhzb_udm]
jy_ptdkjymxb_udm >>  [jy_ajdkhkmxb_udm ,jy_hqckjyhzb_udm]
