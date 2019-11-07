import json
import uuid
import os
from datetime import datetime

from airflow import configuration as conf
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance, DagRun, XCom, Variable
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
            or_(TaskInstance.state == State.SUCCESS, TaskInstance.state == State.FAILED),
        )
        .all()
    )
    return ti


@provide_session
def get_dag_run(task_instance, session=None):
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
def get_unfinished_tasks(dag_run, session=None):
    unfinished_tasks = dag_run.get_task_instances(
        state=State.unfinished(),
        session=session
    )

    return unfinished_tasks


def date_json_serial(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        return obj


@provide_session
def is_first_task(task_instance, dag_run, session=None):
    all_tasks = dag_run.get_task_instances(session=session)
    unfinished_tasks = get_unfinished_tasks(dag_run)

    # Before first task, all tasks are unfinished
    return len(all_tasks) == len(unfinished_tasks)


def get_ids(task_instance, first_task, trace_key, span_key):
    if first_task:
        return uuid.uuid4().hex, uuid.uuid4().hex[16:]

    return task_instance.xcom_pull(key=trace_key), task_instance.xcom_pull(key=span_key)


@provide_session
def sentry_patched_run_raw_task_with_span(task_instance, *args, session=None, **kwargs):
    """
    Create a scope for tagging and breadcrumbs in TaskInstance._run_raw_task.
    """
    hub = Hub.current
    client = hub.client

    with hub.push_scope():
        dag_run = get_dag_run(task_instance)
        run_id = dag_run.run_id
        current_task_id = task_instance.task_id

        add_tagging(task_instance, run_id)
        add_breadcrumbs(task_instance)

        trace_key = "__sentry_trace_id__" + run_id
        span_key = "__sentry_span_id__" + run_id

        first_task = is_first_task(task_instance, dag_run)
        trace_id, parent_span_id = get_ids(
            task_instance, first_task, trace_key, span_key
        )

        task_span = Span(
            op="airflow-task",
            description=current_task_id,
            sampled=True,
            trace_id=trace_id,
            parent_span_id=parent_span_id,
        )

        try:
            with hub.start_span(task_span):
                original_run_raw_task(task_instance, *args, session=session, **kwargs)
        except Exception:
            hub.capture_exception()
            raise
        finally:
            dag_run = get_dag_run(task_instance)
            task_ids = task_instance.task.dag.task_ids
            execution_date = task_instance.execution_date
            dag_id = task_instance.dag_id
            unfinished_tasks = get_unfinished_tasks(dag_run)
            dag_run_state = dag_run.state

            transaction_span_key = "__sentry_parent_span" + run_id

            if first_task:
                transaction_span = Span(
                    op="airflow-dag-run",
                    transaction="dag_run: {} - {}".format(dag_id, execution_date),
                    description=run_id,
                    sampled=True,
                    span_id=parent_span_id,
                    trace_id=trace_id,
                )

                json_transaction_span = json.dumps(
                    transaction_span.to_json(client), default=date_json_serial
                )

                task_instance.xcom_push(
                    key=transaction_span_key, value=json_transaction_span
                )
                task_instance.xcom_push(key=trace_key, value=trace_id)
                task_instance.xcom_push(key=span_key, value=parent_span_id)

            if len(unfinished_tasks) and dag_run_state is not State.FAILED:
                recorded_spans = [
                    json.dumps(span.to_json(client), default=date_json_serial)
                    for span in task_span._span_recorder.finished_spans
                    if span is not None and task_span._span_recorder is not None
                ]

                task_span_key = "__sentry" + current_task_id + run_id
                task_instance.xcom_push(key=task_span_key, value=recorded_spans)

            # All tasks have finished or dag_run has failed
            else:

                def to_json_span(span):
                    json_span = span.to_json(client)

                    json_span["start_timestamp"] = json_span[
                        "start_timestamp"
                    ].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    json_span["timestamp"] = json_span["timestamp"].strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    )

                    return json_span

                last_recorded_spans = [
                    to_json_span(span)
                    for span in task_span._span_recorder.finished_spans
                    if span is not None and task_span._span_recorder is not None
                ]

                for previous_task_id in task_ids:
                    if previous_task_id is not current_task_id:
                        previous_recorded_spans = task_instance.xcom_pull(
                            key="__sentry" + previous_task_id + run_id,
                            task_ids=previous_task_id,
                        )

                        if previous_recorded_spans:
                            span = [
                                json.loads(span)
                                for span in previous_recorded_spans
                                if span is not None
                            ]

                            last_recorded_spans.extend(span)

                json_transaction_span = json.loads(
                    task_instance.xcom_pull(key=transaction_span_key)
                )

                hub.capture_event(
                    {
                        "type": "transaction",
                        "transaction": json_transaction_span["transaction"],
                        "contexts": {"trace": get_trace_context(json_transaction_span)},
                        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "start_timestamp": json_transaction_span["start_timestamp"],
                        "spans": last_recorded_spans,
                    }
                )


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


def get_trace_context(span):
    rv = {
        "trace_id": span['trace_id'],
        "span_id": span['span_id'],
        "parent_span_id": span['parent_span_id'],
        "op": span['op'],
        "description": span['description'],
    }

    return rv

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

        traces_sample_rate = 0.3 if os.environ.get('SENTRY_ENABLE_AIRFLOW_APM') in ["true", "True"] else 0

        if executor_name == "CeleryExecutor":
            from sentry_sdk.integrations.celery import CeleryIntegration

            integrations += [CeleryIntegration()]

        try:
            dsn = None
            conn = self.get_connection(sentry_conn_id)
            dsn = get_dsn(conn)
            sentry_sdk.init(dsn=dsn, integrations=integrations, traces_sample_rate=traces_sample_rate)
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
