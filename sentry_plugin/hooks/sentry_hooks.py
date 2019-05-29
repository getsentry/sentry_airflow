import sentry_sdk
import logging
from airflow.hooks.base_hook import BaseHook
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
from airflow.utils.db import provide_session
from airflow.models import TaskInstance
from sentry_sdk import configure_scope

original_get_context = TaskInstance.get_template_context

@provide_session
def set_tags(self, session=None):
	context = original_get_context(self, session)
	with configure_scope() as scope:
		if context is not None:
			scope.set_tag("task_id", self.task_id)
			scope.set_tag("dag_id", self.dag_id)
			scope.set_tag("execution_date", context["execution_date"])
			scope.set_tag("ds", context["ds"])
			scope.set_tag("operator", self.operator)
	return context

class SentryHook(BaseHook):
	def __init__(self):
		sentry_logging = LoggingIntegration(
			level=logging.INFO,
			event_level=logging.ERROR 
		)
		sentry_celery = CeleryIntegration()
		sentry_sdk.init(integrations=[sentry_logging, sentry_celery])
		TaskInstance.get_template_context = set_tags
