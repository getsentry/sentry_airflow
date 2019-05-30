import sentry_sdk
import logging
from airflow.hooks.base_hook import BaseHook
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
# from airflow.utils.db import provide_session
from airflow.models import TaskInstance
from sentry_sdk import configure_scope, add_breadcrumb

original_get_context = TaskInstance.get_template_context
original_xcomm_push = TaskInstance.xcom_push
original_task_init = TaskInstance.__init__

# @provide_session
# def set_tags(self, session=None):
# 	context = original_get_context(self, session)
# 	with configure_scope() as scope:
# 		if context is not None:
# 			scope.set_tag("task_id", self.task_id)
# 			scope.set_tag("dag_id", self.dag_id)
# 			scope.set_tag("execution_date", context["execution_date"])
# 			scope.set_tag("ds", context["ds"])
# 			scope.set_tag("operator", self.operator)
# 	return context

# def set_crumbs(self, key, value, execution_date=None):
# 	add_breadcrumb(
# 			category="data",
# 			message="Passing result: %s for dag: %s and task: %s." % (value, self.dag_id, self.task_id),
# 			level="info"
# 		)
# 	original_xcomm_push(self, key, value, execution_date)


def add_sentry(self, task, execution_date, state=None):
	original_task_init(self, task, execution_date, state)
	with configure_scope() as scope:
		scope.set_tag("task_id", self.task_id)
		scope.set_tag("dag_id", self.dag_id)
		scope.set_tag("execution_date", self.execution_date)
		scope.set_tag("ds", self.execution_date.strftime("%Y-%m-%d"))
		scope.set_tag("operator", self.operator)

	original_success = self.task.on_success_callback
	original_failure = self.task.on_failure_callback
	def on_success(context, **kwargs):
		if original_success:
			original_success(context, kwargs)
		with configure_scope() as scope:
			scope.set_tag("Hello", self.task_id)
		add_breadcrumb(
			category="data",
			message="Dag: %s, with Task: %s Executed on: %s" % (self.dag_id, self.task_id, self.execution_date),
			level="error"
		)

	def on_failure(context, **kwargs):
		if original_failure:
			original_failure(context, kwargs)
		add_breadcrumb(
			category="data",
			message="Dag: %s, with Task: %s Executed on: %s" % (self.dag_id, self.task_id, self.execution_date),
			level="error"
		)



	self.task.on_success_callback = on_success
	self.task.on_failure_callback = on_failure




class SentryHook(BaseHook):
	def __init__(self):
		sentry_logging = LoggingIntegration(
			level=logging.INFO,
			event_level=logging.ERROR 
		)
		sentry_celery = CeleryIntegration()

		self.conn_id = None
		self.dsn = None

		try:
			self.conn_id = self.get_connection("sentry_dsn")
			self.dsn = self.conn_id.host
			sentry_sdk.init(dsn=self.dsn, integrations=[sentry_celery])
		except:
			sentry_sdk.init(integrations=[sentry_celery])
		TaskInstance.__init__ = add_sentry
		# TaskInstance.get_template_context = set_tags
		# TaskInstance.xcomm_push = set_crumbs
