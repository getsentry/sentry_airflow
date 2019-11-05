from airflow.plugins_manager import AirflowPlugin
from sentry_airflow.hooks.sentry_hook import SentryHook


class SentryPlugin(AirflowPlugin):
    name = "SentryPlugin"
    hooks = [SentryHook]
    operators = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
