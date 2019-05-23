from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance
from airflow.utils.db import provide_session

from sentry_sdk import (
    configure_scope,
    push_scope,
    capture_exception,
    add_breadcrumb,
    init,
)
from sentry_sdk.integrations.logging import ignore_logger
from sentry_sdk.integrations.flask import FlaskIntegration
from sqlalchemy import exc

SCOPE_TAGS = frozenset(("task_id", "dag_id", "execution_date", "operator"))
SCOPE_CRUMBS = frozenset(
    ("dag_id", "task_id", "execution_date", "state", "operator", "duration")
)


@provide_session
def get_task_instances(dag_id, task_ids, execution_date, session=None):
    """
    Retrieve attribute from task.
    """
    if session is None or not task_ids:
        return []
    TI = TaskInstance
    ti = (
        session.query(TI)
        .filter(
            TI.dag_id == dag_id,
            TI.task_id.in_(task_ids),
            TI.execution_date == execution_date,
        )
        .all()
    )
    return ti


def add_tagging(task_instance):
    """
    Add customized tagging to TaskInstances.
    """
    with configure_scope() as scope:
        for tag_name in SCOPE_TAGS:
            attribute = getattr(task_instance, tag_name)
            if tag_name == "operator":
                attribute = task_instance.task.__class__.__name__
            scope.set_tag(tag_name, attribute)


def add_breadcrumbs(task_instance, session=None):
    """
    Add customized breadcrumbs to TaskInstances.
    """
    task_ids = task_instance.task.dag.task_ids
    execution_date = task_instance.execution_date
    dag_id = task_instance.dag_id
    task_instances = get_task_instances(dag_id, task_ids, execution_date, session)
    for ti in task_instances:
        if ti is None or ti.state is None:
            continue
        data = {}
        for crumb_tag in SCOPE_CRUMBS:
            data[crumb_tag] = getattr(ti, crumb_tag)

        add_breadcrumb(category="upstream_tasks", data=data, level="info")


@provide_session
def add_sentry(task_instance, *args, session=None, **kwargs):
    """
    Create a scope for tagging and breadcrumbs in TaskInstance._run_raw_task.
    """
    # Avoid leaking tags by using push_scope.
    with push_scope():
        add_tagging(task_instance)
        add_breadcrumbs(task_instance, session)
        try:
            original_run_raw_task(task_instance, *args, session=session, **kwargs)
        except Exception:
            capture_exception()
            raise


class SentryHook(BaseHook):
    """
    Wrap around the Sentry SDK.
    """

    def __init__(self, sentry_conn_id=None):
        ignore_logger("airflow.task")
        ignore_logger("airflow.jobs.backfill_job.BackfillJob")
        executor_name = configuration.conf.get("core", "EXECUTOR")

        sentry_flask = FlaskIntegration()
        integrations = [sentry_flask]

        if executor_name == "CeleryExecutor":
            from sentry_sdk.integrations.celery import CeleryIntegration
            from sentry_sdk.integrations.tornado import TornadoIntegration

            sentry_celery = CeleryIntegration()
            sentry_tornado = TornadoIntegration()
            integrations += [sentry_celery]

        self.conn_id = None
        self.dsn = None

        try:
            if sentry_conn_id is None:
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

        TaskInstance._run_raw_task = add_sentry
        TaskInstance._sentry_integration_ = True


if not getattr(TaskInstance, "_sentry_integration_", False):
    original_run_raw_task = TaskInstance._run_raw_task
    SentryHook()
