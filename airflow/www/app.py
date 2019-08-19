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
from typing import Any

import six
from flask import Flask
from flask_admin import Admin, base
from flask_caching import Cache
from flask_wtf.csrf import CSRFProtect
from six.moves.urllib.parse import urlparse
from werkzeug.wsgi import DispatcherMiddleware
from werkzeug.contrib.fixers import ProxyFix
from flask_babelex import Babel, gettext as _, lazy_gettext

import airflow
from airflow import configuration as conf
from airflow import models, LoggingMixin
from airflow.models.connection import Connection
from airflow.models.downloadconfig import DownloadConfig
from airflow.settings import Session

from airflow.www.blueprints import routes
from airflow.logging_config import configure_logging
from airflow import jobs
from airflow import settings
from airflow import configuration
from airflow.utils.net import get_hostname

csrf = CSRFProtect()


def create_app(config=None, session=None, testing=False):
    app = Flask(__name__)
    if configuration.conf.getboolean('webserver', 'ENABLE_PROXY_FIX'):
        app.wsgi_app = ProxyFix(app.wsgi_app)
    app.secret_key = configuration.conf.get('webserver', 'SECRET_KEY')
    app.config['LOGIN_DISABLED'] = not configuration.conf.getboolean(
        'webserver', 'AUTHENTICATE')

    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SECURE'] = conf.getboolean('webserver', 'COOKIE_SECURE')
    app.config['SESSION_COOKIE_SAMESITE'] = conf.get('webserver', 'COOKIE_SAMESITE')
    app.config['BABEL_DEFAULT_LOCALE'] = 'zh_CN'
    
    babel = Babel(app)
    @babel.localeselector
    def get_locale():
        if session:
            session['lang'] = 'zh_CN'
        return 'zh_CN'
    if config:
        app.config.from_mapping(config)
        
    # @babel.timezoneselector
    # def get_timezone():
    #     user = getattr(g, 'user', None)
    #     if user is not None:
    #         return user.timezone

    csrf.init_app(app)

    app.config['TESTING'] = testing

    airflow.load_login()
    airflow.login.LOGIN_MANAGER.init_app(app)

    from airflow import api
    api.load_auth()
    api.API_AUTH.api_auth.init_app(app)

    # flake8: noqa: F841
    cache = Cache(app=app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'})

    app.register_blueprint(routes)

    configure_logging()

    with app.app_context():
        from airflow.www import views

        admin = Admin(
            app, name=lazy_gettext('Airflow'),
            static_url_path='/admin',
            index_view=views.HomeView(endpoint='', url='/admin', name=lazy_gettext("DAGs")),
            template_mode='bootstrap3',
        )
        av = admin.add_view
        vs = views
        av(vs.Airflow(name=lazy_gettext('DAGs'), category=lazy_gettext('DAGs')))

        if not conf.getboolean('core', 'secure_mode'):
            av(vs.QueryView(name=lazy_gettext('Ad Hoc Query'), category=lazy_gettext("Data Profiling")))
            av(vs.ChartModelView(
                models.Chart, Session, name=lazy_gettext("Charts"), category=lazy_gettext("Data Profiling")))
        av(vs.KnownEventView(
            models.KnownEvent,
            Session, name=lazy_gettext("Known Events"), category=lazy_gettext("Data Profiling")))
        # av(vs.SlaMissModelView(
        #     models.SlaMiss,
        #     Session, name=lazy_gettext("SLA Misses"), category=lazy_gettext("Browse")))
        av(vs.TaskInstanceModelView(models.TaskInstance,
            Session, name=lazy_gettext("Task Instances"), category=lazy_gettext("Browse")))
        av(vs.LogModelView(
            models.Log, Session, name=lazy_gettext("Logs"), category=lazy_gettext("Browse")))
        # av(vs.JobModelView(
        #     jobs.BaseJob, Session, name=lazy_gettext("Jobs"), category=lazy_gettext("Browse")))
        # av(vs.PoolModelView(
        #     models.Pool, Session, name=lazy_gettext("Pools"), category=lazy_gettext("Admin")))
        av(vs.ConfigurationView(
            name=lazy_gettext('Configuration'), category=lazy_gettext("Admin")))
        av(vs.UserModelView(
            models.User, Session, name=lazy_gettext("Users"), category=lazy_gettext("Admin")))
        av(vs.ConnectionModelView(
            Connection, Session, name=lazy_gettext("Connections"), category=lazy_gettext("Admin")))
        # av(vs.DownLoadConfigView(
        #     DownloadConfig, Session, name=lazy_gettext("DownLoadConfigs"), category=lazy_gettext("Admin")))
        av(vs.VariableView(
            models.Variable, Session, name=lazy_gettext("Variables"), category=lazy_gettext("Admin")))
        # av(vs.XComView(
        #     models.XCom, Session, name=lazy_gettext("XComs"), category=lazy_gettext("Admin")))

        admin.add_link(base.MenuLink(
            category=lazy_gettext('Docs'), name=lazy_gettext('Documentation'),
            url='https://airflow.apache.org/'))
        admin.add_link(
            base.MenuLink(category=lazy_gettext('Docs'),
                          name=lazy_gettext('GitHub'),
                          url='https://github.com/apache/airflow'))

        av(vs.VersionView(name=lazy_gettext('Version'), category=lazy_gettext("About")))

        av(vs.DagRunModelView(
            models.DagRun, Session, name=lazy_gettext("DAG Runs"), category=lazy_gettext("Browse")))
        av(vs.DagModelView(models.DagModel, Session, name=None))
        # Hack to not add this view to the menu
        admin._menu = admin._menu[:-1]

        def integrate_plugins():
            """Integrate plugins to the context"""
            log = LoggingMixin().log
            from airflow.plugins_manager import (
                admin_views, flask_blueprints, menu_links)
            for v in admin_views:
                log.debug('Adding view %s', v.name)
                admin.add_view(v)
            for bp in flask_blueprints:
                log.debug("Adding blueprint %s:%s", bp["name"], bp["blueprint"].import_name)
                app.register_blueprint(bp["blueprint"])
            for ml in sorted(menu_links, key=lambda x: x.name):
                log.debug('Adding menu link %s', ml.name)
                admin.add_link(ml)

        integrate_plugins()

        import airflow.www.api.experimental.endpoints as e
        # required for testing purposes otherwise the module retains
        # a link to the default_auth
        if app.config['TESTING']:
            six.moves.reload_module(e)

        app.register_blueprint(e.api_experimental, url_prefix='/api/experimental')

        @app.context_processor
        def jinja_globals():
            return {
                'hostname': get_hostname(),
                'navbar_color': configuration.get('webserver', 'NAVBAR_COLOR'),
            }

        @app.teardown_appcontext
        def shutdown_session(exception=None):
            settings.Session.remove()

        return app


app = None  # type: Any


def root_app(env, resp):
    resp('404 Not Found', [('Content-Type', 'text/plain')])
    return [b'Apache Airflow is not at this location']


def cached_app(config=None, testing=False):
    global app
    if not app:
        base_url = urlparse(configuration.conf.get('webserver', 'base_url'))[2]
        if not base_url or base_url == '/':
            base_url = ""

        app = create_app(config, testing)
        app = DispatcherMiddleware(root_app, {base_url: app})
    return app
