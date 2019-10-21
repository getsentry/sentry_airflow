import uuid

from airflow import configuration as conf
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance, DagRun, XCom
from airflow.utils.db import provide_session
from airflow.utils.state import State

import sentry_sdk
from sentry_sdk.hub import Hub
from sentry_sdk.tracing import Span
from sentry_sdk.integrations.logging import ignore_logger
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

from sqlalchemy import exc, or_

SCOPE_TAGS = frozenset(("task_id", "dag_id", "execution_date", "operator"))
SCOPE_CRUMBS = frozenset(
    ("dag_id", "task_id", "execution_date", "state", "operator", "duration")
)


def get_task_instances(dag_id, task_ids, execution_date, session):
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
            or_(TaskInstance.state == State.SUCCESS, TaskInstance.state == State.FAILED),
        )
        .all()
    )
    return ti


def get_dag_run(task_instance, session):
    dag_run = (
        session.query(DagRun)
        .filter_by(dag_id=task_instance.dag_id, execution_date=task_instance.execution_date)
        .first()
    )

    return dag_run


def add_tagging(task_instance, run_id):
    """
    Add customized tagging to TaskInstances.
    """
    hub = Hub.current

    with hub.configure_scope() as scope:
        scope.set_tag("run_id", run_id)

        for tag_name in SCOPE_TAGS:
            attribute = getattr(task_instance, tag_name)
            if tag_name == "operator":
                attribute = task_instance.task.__class__.__name__
            scope.set_tag(tag_name, attribute)


def add_breadcrumbs(task_instance, session):
    """
    Add customized breadcrumbs to TaskInstances.
    """
    hub = Hub.current

    task_ids = task_instance.task.dag.task_ids
    execution_date = task_instance.execution_date
    dag_id = task_instance.dag_id
    task_instances = get_task_instances(dag_id, task_ids, execution_date, session)
    for ti in task_instances:
        data = {}
        for crumb_tag in SCOPE_CRUMBS:
            data[crumb_tag] = getattr(ti, crumb_tag)

        hub.add_breadcrumb(category="completed_tasks", data=data, level="info")


def get_unfinished_tasks(dag_run, session):
    unfinished_tasks = dag_run.get_task_instances(
        state=State.unfinished(),
        session=session
    )

    return unfinished_tasks

def get_traceparent(task_instance, dag_run, run_id, session):
    dag_id = task_instance.dag_id

    all_tasks = dag_run.get_task_instances(session=session)
    unfinished_tasks = get_unfinished_tasks(dag_run, session)

    # Before first task, all tasks are unfinished
    if len(all_tasks) == len(unfinished_tasks):
        parent_span = Span(op="airflow", transaction="dag_id: {}".format(dag_id), sampled=True)

        traceparent = parent_span.to_traceparent()

        task_instance.xcom_push(key=run_id, value=traceparent)
        print("Xcom stuff")
        print(session.query(XCom).all())
        print(task_instance.xcom_pull(key=run_id))
        return traceparent
    
    print("traceparent")
    print(session.query(XCom).all())
    traceparent = task_instance.xcom_pull(key=run_id)
    print(traceparent)
    task_instance.xcom_push(key=run_id, value=traceparent)
    return traceparent


@provide_session
def sentry_patched_run_raw_task(task_instance, *args, session=None, **kwargs):
    """
    Create a scope for tagging and breadcrumbs in TaskInstance._run_raw_task.
    """
    hub = Hub.current

    # Avoid leaking tags by using push_scope.
    with hub.push_scope():
        dag_run = get_dag_run(task_instance, session)
        run_id = dag_run.run_id

        add_tagging(task_instance, run_id)
        add_breadcrumbs(task_instance, session)

        traceparent = get_traceparent(task_instance, dag_run, run_id, session)

        parent_span = Span.from_traceparent(traceparent)

        try:
            with hub.start_span(parent_span.new_span(same_process_as_parent=False)):
                original_run_raw_task(task_instance, *args, session=session, **kwargs)
        except Exception:
            hub.capture_exception()
            raise
        finally:
            unfinished_tasks = get_unfinished_tasks(dag_run, session)

            if len(unfinished_tasks) == 0:
                parent_span.finish(hub=hub) 


def get_dsn(conn):
    if None in (conn.conn_type, conn.login):
        return conn.host

    dsn = '{conn.conn_type}://{conn.login}@{conn.host}/{conn.schema}'.format(conn=conn)
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

        if executor_name == "CeleryExecutor":
            from sentry_sdk.integrations.celery import CeleryIntegration

            integrations += [CeleryIntegration()]

        try:
            dsn = None
            conn = self.get_connection(sentry_conn_id)
            dsn = get_dsn(conn)
            sentry_sdk.init(dsn=dsn, integrations=integrations, debug=True, traces_sample_rate=1)
        except (AirflowException, exc.OperationalError, exc.ProgrammingError):
            self.log.debug("Sentry defaulting to environment variable.")
            init(integrations=integrations)

        TaskInstance._run_raw_task = sentry_patched_run_raw_task
        TaskInstance._sentry_integration_ = True


if not getattr(TaskInstance, "_sentry_integration_", False):
    original_run_raw_task = TaskInstance._run_raw_task
    SentryHook()
