import logging

from flask import request

from airflow import settings
from airflow.hooks.base_hook import BaseHook
from airflow.utils.db import provide_session
from airflow.models import DagBag
from airflow.models import TaskInstance

from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.logging import ignore_logger
from sentry_sdk import configure_scope, add_breadcrumb, init

original_task_init = TaskInstance.__init__
original_clear_xcom = TaskInstance.clear_xcom_data

@provide_session
def new_clear_xcom(self, session=None):
	'''
	Add breadcrumbs just before task is executed.
	'''
	for t in self.task.get_flat_relatives(upstream=True):
		state = TaskInstance(t, self.execution_date).current_state()
		add_breadcrumb(
			category="data",
			message="Upstream Task: %s was %s" % (t.task_id, state),
			level="info"
		)
	original_clear_xcom(self, session)

def add_sentry(self, task, execution_date, state=None):
	'''
	Change the TaskInstance init function to add costumized tagging.
	'''
	original_task_init(self, task, execution_date, state)
	with configure_scope() as scope:
		scope.set_tag("task_id", self.task_id)
		scope.set_tag("dag_id", self.dag_id)
		scope.set_tag("execution_date", self.execution_date)
		scope.set_tag("ds", self.execution_date.strftime("%Y-%m-%d"))
		scope.set_tag("operator", self.operator)

class SentryHook(BaseHook):
	'''
	Wrap around the Sentry SDK.
	'''
	def __init__(self):
		sentry_celery = CeleryIntegration()
		integrations = [sentry_celery]
		ignore_logger("airflow.task")

		self.conn_id = None
		self.dsn = None

		try:
			self.conn_id = self.get_connection("sentry_dsn")
			self.dsn = self.conn_id.host
			init(dsn=self.dsn, integrations=integrations)
		except:
			init(integrations=integrations)

		TaskInstance.__init__ = add_sentry
		TaskInstance.clear_xcom_data = new_clear_xcom
