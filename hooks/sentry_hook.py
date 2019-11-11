import json
import uuid
import os
from datetime import datetime

from airflow import configuration as conf
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance, DagRun
from airflow.utils.db import provide_session
from airflow.utils.state import State

import sentry_sdk
from sentry_sdk.hub import Hub
from sentry_sdk.integrations.logging import ignore_logger
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

from sqlalchemy import exc, or_

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
    ti = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.task_id.in_(task_ids),
            TaskInstance.execution_date == execution_date,
            or_(
                TaskInstance.state == State.SUCCESS, TaskInstance.state == State.FAILED
            ),
        )
        .all()
    )
    return ti


@provide_session
def get_dag_run(task_instance, session=None):
    dag_run = (
        session.query(DagRun)
        .filter_by(
            dag_id=task_instance.dag_id, execution_date=task_instance.execution_date
        )
        .first()
    )

    return dag_run


def add_tagging(task_instance, run_id):
    """
    Add customized tagging to TaskInstances.
    """
    hub = Hub.current
    executor_name = conf.get("core", "EXECUTOR")

    with hub.configure_scope() as scope:
        scope.set_tag("run_id", run_id)
        scope.set_tag("executor", executor_name)

        for tag_name in SCOPE_TAGS:
            attribute = getattr(task_instance, tag_name)
            if tag_name == "operator":
                attribute = task_instance.task.__class__.__name__
            scope.set_tag(tag_name, attribute)


@provide_session
def add_breadcrumbs(task_instance, session=None):
    """
    Add customized breadcrumbs to TaskInstances.
    """
    hub = Hub.current

    task_ids = task_instance.task.dag.task_ids
    execution_date = task_instance.execution_date
    dag_id = task_instance.dag_id
    task_instances = get_task_instances(dag_id, task_ids, execution_date)
    for ti in task_instances:
        data = {}
        for crumb_tag in SCOPE_CRUMBS:
            data[crumb_tag] = getattr(ti, crumb_tag)

        hub.add_breadcrumb(category="completed_tasks", data=data, level="info")


@provide_session
def sentry_patched_run_raw_task_with_span(task_instance, *args, session=None, **kwargs):
    """
    Create a scope for tagging and breadcrumbs in TaskInstance._run_raw_task.
    """
    hub = Hub.current
    # Avoid leaking tags by using push_scope.
    with hub.push_scope():
        dag_run = get_dag_run(task_instance)
        task_id = task_instance.task_id
        dag_id = task_instance.dag_id
        run_id = dag_run.run_id

        add_tagging(task_instance, run_id)
        add_breadcrumbs(task_instance, session)
        try:
            with hub.start_span(
                op="airflow-task",
                transaction="task_run: {} - {} - {}".format(
                    task_id, dag_id, execution_date
                ),
            ):
                original_run_raw_task(task_instance, *args, session=session, **kwargs)
        except Exception:
            hub.capture_exception()
            raise


@provide_session
def sentry_patched_run_raw_task(task_instance, *args, session=None, **kwargs):
    """
    Create a scope for tagging and breadcrumbs in TaskInstance._run_raw_task.
    """
    hub = Hub.current
    # Avoid leaking tags by using push_scope.
    with hub.push_scope():
        dag_run = get_dag_run(task_instance)
        run_id = dag_run.run_id

        add_tagging(task_instance, run_id)
        add_breadcrumbs(task_instance, session)
        try:
            original_run_raw_task(task_instance, *args, session=session, **kwargs)
        except Exception:
            hub.capture_exception()
            raise


def get_dsn(conn):
    if None in (conn.conn_type, conn.login):
        return conn.host

    dsn = "{conn.conn_type}://{conn.login}@{conn.host}/{conn.schema}".format(conn=conn)
    return dsn


class SentryHook(BaseHook):
    """
    Wrap around the Sentry SDK.
    """

    def __init__(self, sentry_conn_id="sentry_dsn"):
        ignore_logger("airflow.task")
        ignore_logger("airflow.jobs.backfill_job.BackfillJob")
        executor_name = conf.get("core", "EXECUTOR")

        integrations = [FlaskIntegration(), SqlalchemyIntegration()]

        traces_sample_rate = (
            1
            if os.environ.get("SENTRY_ENABLE_AIRFLOW_APM") in ["true", "True"]
            else 0
        )

        if executor_name == "CeleryExecutor":
            from sentry_sdk.integrations.celery import CeleryIntegration

            integrations += [CeleryIntegration()]

        try:
            dsn = None
            conn = self.get_connection(sentry_conn_id)
            dsn = get_dsn(conn)
            sentry_sdk.init(
                dsn=dsn,
                integrations=integrations,
                traces_sample_rate=traces_sample_rate,
            )

        except (AirflowException, exc.OperationalError, exc.ProgrammingError):
            self.log.debug("Sentry defaulting to environment variable.")
            sentry_sdk.init(integrations=integrations)

        if traces_sample_rate is 0:
            TaskInstance._run_raw_task = sentry_patched_run_raw_task
        else:
            self.log.debug("Sentry sending APM spans.")
            TaskInstance._run_raw_task = sentry_patched_run_raw_task_with_span

        TaskInstance._sentry_integration_ = True


if not getattr(TaskInstance, "_sentry_integration_", False):
    original_run_raw_task = TaskInstance._run_raw_task
    SentryHook()
