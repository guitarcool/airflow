from flask_babel import gettext as _;
def _useless():
    # list_columns = [_('dag_id'), _('task_id'), _('execution_date'), _('email_sent'), _('timestamp')]
    # add_columns = [_('dag_id'), _('task_id'), _('execution_date'), _('email_sent'), _('timestamp')]
    # edit_columns = [_('dag_id'), _('task_id'), _('execution_date'), _('email_sent'), _('timestamp')]
    # search_columns = [_('dag_id'), _('task_id'), _('email_sent'), _('timestamp'), _('execution_date')]
    # base_order = (_('execution_date'), _('desc'))

    # base_permissions = [_('can_add'), _('can_list'), _('can_edit'), _('can_delete')]

    # search_columns = [_('key'), _('value'), _('timestamp'), _('execution_date'), _('task_id'), _('dag_id')]
    # list_columns = [_('key'), _('value'), _('timestamp'), _('execution_date'), _('task_id'), _('dag_id')]
    # add_columns = [_('key'), _('value'), _('execution_date'), _('task_id'), _('dag_id')]
    # edit_columns = [_('key'), _('value'), _('execution_date'), _('task_id'), _('dag_id')]
    # base_order = (_('execution_date'), _('desc'))

    # list_columns = [_('conn_id'), _('conn_type'), _('host'), _('port'), _('is_encrypted'),
    #                 _('is_extra_encrypted')]

    # list_columns = [_('pool'), _('slots'), _('used_slots'), _('queued_slots')]
    # add_columns = [_('pool'), _('slots'), _('description')]
    # edit_columns = [_('pool'), _('slots'), _('description')]

    # base_permissions = [_('can_add'), _('can_list'), _('can_edit'), _('can_delete'), _('can_varimport')]

    # list_columns = [_('key'), _('val'), _('is_encrypted')]
    # add_columns = [_('key'), _('val'), _('is_encrypted')]
    # edit_columns = [_('key'), _('val')]
    # search_columns = [_('key'), _('val')]

    # base_order = (_('key'), _('asc'))

    # base_permissions = [_('can_list')]

    # list_columns = [_('id'), _('dag_id'), _('state'), _('job_type'), _('start_date'),
    #                 _('end_date'), _('latest_heartbeat'),
    #                 _('executor_class'), _('hostname'), _('unixname')]
    # search_columns = [_('id'), _('dag_id'), _('state'), _('job_type'), _('start_date'),
    #                   _('end_date'), _('latest_heartbeat'), _('executor_class'),
    #                   _('hostname'), _('unixname')]

    # base_order = (_('start_date'), _('desc'))

    # base_permissions = [_('can_list')]

    # list_columns = [_('state'), _('dag_id'), _('execution_date'), _('run_id'), _('external_trigger')]
    # search_columns = [_('state'), _('dag_id'), _('execution_date'), _('run_id'), _('external_trigger')]

    # base_order = (_('execution_date'), _('desc'))

    _('First Name')
    _('Last Name')
    _('User Name')
    _('Email')
    _('Is Active?')
    _('Role')
    _('Airflow'),
    _('DagModelView'),
    _('Browse'),
    _('DAG Runs'),
    _('DagRunModelView'),
    _('Task Instances'),
    _('TaskInstanceModelView'),
    _('SLA Misses'),
    _('SlaMissModelView'),
    _('Jobs'),
    _('JobModelView'),
    _('Logs'),
    _('LogModelView'),
    _('Docs'),
    _('Documentation'),
    _('Github'),
    _('About'),
    _('Version'),
    _('VersionView'),

    _('Admin'),
    _('Configurations'),
    _('ConfigurationView'),
    _('Connections'),
    _('ConnectionModelView'),
    _('Pools'),
    _('PoolModelView'),
    _('Variables'),
    _('VariableModelView'),
    _('XComs'),
    _('XComModelView'),

    # _('menu_access'),
    # _('can_index'),
    # _('can_list'),
    # _('can_show'),
    # _('can_chart'),
    # _('can_dag_stats'),
    # _('can_dag_details'),
    # _('can_task_stats'),
    # _('can_code'),
    # _('can_log'),
    # _('can_get_logs_with_metadata'),
    # _('can_tries'),
    # _('can_graph'),
    # _('can_tree'),
    # _('can_task'),
    # _('can_task_instances'),
    # _('can_xcom'),
    # _('can_gantt'),
    # _('can_landing_times'),
    # _('can_duration'),
    # _('can_blocked'),
    # _('can_rendered'),
    # _('can_version'),


    # _('can_dagrun_clear'),
    # _('can_run'),
    # _('can_trigger'),
    # _('can_add'),
    # _('can_edit'),
    # _('can_delete'),
    # _('can_paused'),
    # _('can_refresh'),
    # _('can_success'),
    # _('muldelete'),
    # _('set_failed'),
    # _('set_running'),
    # _('set_success'),
    # _('clear'),
    # _('can_clear'),


    # _('can_conf'),
    # _('can_varimport'),

    _('dttm')
    _('base_date'),
    _('num_runs'),
    _('execution_date'),
    _('dr_choices'),
    _('dr_state'),

    _('Security'),
    _('List Users'),
    _('List Roles'),
    _("User's Statistics"),
    _('Base Permissions'),
    _('Views/Menus'),
    _('Permission on Views/Menus'),

    # search filter
    _('Created by'),
    _('Changed by'),
    _('Last login'),
    _('Login count'),
    _('Failed login count'),
    _('Created on'),
    _('Changed on'),
    _('Created'),
    _('Changed')

    _('Permission'),
    _('View/Menu'),

    #form
    _('Name'),
    _('Record Count'),
    _('Add Filter'),
    _('Page size')
    #List Permissions on Views/Menus
