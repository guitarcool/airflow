from flask_babel import gettext as _;
def _useless():
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

    #/admin/connection
    _('List'),
    _('Create'),
    _('With selected'),
    _('Conn Id'),
    _('Conn Type'),
    _('Host'),
    _('Port'),
    _('Is Encrypted'),
    _('Is Extra Encrypted')

    #/admin/pool
    _('Pool'),
    _('Slots'),
    _('Used Slots'),
    _('Queued Slots'),

    #/admin/knownevent
    _('Known Event'),
    _('Label'),
    _('Event Type'),
    _('Start Date'),
    _('End Date'),
    _('Reported By'),
    _('Description'),

    #/admin/slamiss
    _('Sla Misses'),
    _('Dag Id'),
    _('Task Id'),
    _('Execution Date'),
    _('Email Sent'),
    _('Timestamp'),

    #/admin/taskinstance
    _('State'),
    _('Dag Id'),
    _('Task Id'),
    _('Operator'),
    _('Duration'),
    _('Job Id'),
    _('Hostname'),
    _('Unixname'),
    _('Priority Weight'),
    _('Queue'),
    _('Queued Dttm'),
    _('Try Number'),
    _('Log Url'),

    #/admin/log
    _('Id'),
    _('Dttm'),
    _('Dag Id'),
    _('Task Id'),
    _('Event'),
    _('Execution Date'),
    _('Owner'),
    _('Extra'),

    #/admin/basejob
    _('Id'),
    _('Dag Id'),
    _('State'),
    _('Job Type'),
    _('Start Date'),
    _('End Date'),
    _('Latest Heartbeat'),
    _('Executor Class'),
    _('Hostname'),
    _('Unixname'),

    #/admin/dagrun
    _('Dag Run'),
    _('State'),
    _('Dag Id'),
    _('Execution Date'),
    _('Run Id'),
    _('External Trigger'),

    #/admin/pool
    
    #/admin/configurationview
    _('Airflow Configuration'),
    _('Username'),
    _('Email'),
    _('Superuser'),
    _('Known Events'),
    _('Charts'),

    #/admin/downloadconfig
    _('Download Config'),
    _('Sys Id'),
    _('Connection'),
    _('Src Path'),
    _('Dst Path'),
    _('Flag To Download'),
    _('Time To Download'),

    #/admin/variable
    _('Variable'),
    _('Key'),
    _('Val'),

    #/admin/xcom
    _('Xcoms'),
    _('Key'),
    _('Value'),
    _('Timestamp'),
    _('Execution Date'),
    _('Task Id'),
    _('Dag Id'),

    #form
    _('upstream_failed'),