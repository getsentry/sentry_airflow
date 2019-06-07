import logging

from flask import request

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import DagBag
from airflow.models import TaskInstance
from airflow.utils.db import provide_session

from sentry_sdk import configure_scope, add_breadcrumb, init
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.logging import ignore_logger
from sqlalchemy import exc

SCOPE_TAGS = frozenset(("task_id", "dag_id", "execution_date", "ds", "operator"))


@provide_session
def get_task_instance_attr(self, task_id, attr, session=None):
    """
    Retrieve attribute from task.
    """
    if session is None:
        return None
    TI = TaskInstance
    ti = (
        session.query(TI)
        .filter(
            TI.dag_id == self.dag_id,
            TI.task_id == task_id,
            TI.execution_date == self.execution_date,
        )
        .all()
    )
    if ti:
        attr = getattr(ti[0], attr)
    else:
        attr = None
    return attr


@property
def ds(self):
    """
    Date stamp for task object.
    """
    return self.execution_date.strftime("%Y-%m-%d")


@provide_session
def new_clear_xcom(self, session=None):
    """
    Add breadcrumbs just before task is executed.
    """
    for task in self.task.get_flat_relatives(upstream=True):
        task_id = task.task_id
        state = get_task_instance_attr(self, task_id, "state")
        operation = get_task_instance_attr(self, task_id, "operator")
        add_breadcrumb(
            category="data",
            message="Upstream Task: {}, State: {}, Operation: {}".format(
                task_id, state, operation
            ),
            level="info",
        )
    original_clear_xcom(self, session)


def add_sentry(self, *args, **kwargs):
    """
    Change the TaskInstance init function to add customized tagging.
    """
    original_task_init(self, *args, **kwargs)
    self.operator = self.task.__class__.__name__
    with configure_scope() as scope:
        for tag_name in SCOPE_TAGS:
            scope.set_tag(tag_name, getattr(self, tag_name))


class SentryHook(BaseHook):
    """
    Wrap around the Sentry SDK.
    """

    def __init__(self, sentry_conn_id=None):
        sentry_celery = CeleryIntegration()
        integrations = [sentry_celery]
        ignore_logger("airflow.task")

        self.conn_id = None
        self.dsn = None

        try:
            if sentry_conn_id == None:
                self.conn_id = self.get_connection("sentry_dsn")
            else:
                self.conn_id = self.get_connection(sentry_conn_id)
            self.dsn = self.conn_id.host
            init(dsn=self.dsn, integrations=integrations)
        except (AirflowException, exc.OperationalError):
            self.log.warning(
                "Connection was not found, defaulting to environment variable."
            )
            init(integrations=integrations)

        TaskInstance.__init__ = add_sentry
        TaskInstance.clear_xcom_data = new_clear_xcom
        TaskInstance.ds = ds
        TaskInstance._sentry_integration_ = True


if not getattr(TaskInstance, "_sentry_integration_", False):
    original_task_init = TaskInstance.__init__
    original_clear_xcom = TaskInstance.clear_xcom_data
    SentryHook()
