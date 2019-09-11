import copy
import datetime
import unittest
from unittest import mock
from unittest.mock import Mock, MagicMock
from freezegun import freeze_time

from airflow.models import TaskInstance
from airflow.models import Connection
from airflow.settings import Session
from airflow.utils import timezone
from airflow.utils.state import State

from sentry_sdk import configure_scope

from sentry_airflow.hooks.sentry_hook import (
    SentryHook,
    get_task_instances,
    add_tagging,
    add_breadcrumbs,
    get_dsn,
)

EXECUTION_DATE = timezone.utcnow()
DAG_ID = "test_dag"
TASK_ID = "test_task"
OPERATOR = "test_operator"
STATE = State.SUCCESS
DURATION = None
TEST_SCOPE = {
    "dag_id": DAG_ID,
    "task_id": TASK_ID,
    "execution_date": EXECUTION_DATE,
    "operator": OPERATOR,
}
TASK_DATA = TEST_SCOPE.copy()
TASK_DATA.update({"state": STATE, "operator": OPERATOR, "duration": DURATION})
CRUMB_DATE = datetime.datetime(2019, 5, 15)
CRUMB = {
    "timestamp": CRUMB_DATE,
    "type": "default",
    "category": "completed_tasks",
    "data": TASK_DATA,
    "level": "info",
}

class MockQuery:
    """
    Mock Query for when session is called.
    """

    def __init__(self, task_instance):
        task_instance.state = STATE
        self.arr = [task_instance]

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return self.arr

    def first(self):
        return self.arr[0]

    def delete(self):
        pass

class TestSentryHook(unittest.TestCase):
    @mock.patch("sentry_airflow.hooks.sentry_hook.SentryHook.get_connection")
    def setUp(self, mock_get_connection):
        self.assertEqual(TaskInstance._sentry_integration_, True)
        mock_get_connection.return_value = Connection(host="https://foo@sentry.io/123")
        self.sentry_hook = SentryHook("sentry_default")
        self.assertEqual(TaskInstance._sentry_integration_, True)

        self.dag = Mock(dag_id=DAG_ID)
        self.dag.task_ids = [TASK_ID]

        self.task = Mock(dag=self.dag, dag_id=DAG_ID, task_id=TASK_ID)
        self.task.__class__.__name__ = OPERATOR

        self.session = Session()
        self.ti = TaskInstance(self.task, execution_date=EXECUTION_DATE)
        self.ti.operator = OPERATOR
        self.session.query = MagicMock(return_value=MockQuery(self.ti))

    def test_add_tags(self):
        """
        Test adding tags.
        """
        add_tagging(self.ti)
        with configure_scope() as scope:
            for key, value in scope._tags.items():
                self.assertEqual(TEST_SCOPE[key], value)

    def test_get_task_instances(self):
        """
        Test getting instances that have already completed.
        """
        ti = get_task_instances(DAG_ID, [TASK_ID], EXECUTION_DATE, self.session)
        self.assertEqual(ti[0], self.ti)

    @freeze_time(CRUMB_DATE.isoformat())
    def test_add_breadcrumbs(self):
        """
        Test adding breadcrumbs.
        """
        add_breadcrumbs(self.ti, self.session)

        with configure_scope() as scope:
            test_crumb = scope._breadcrumbs.pop()
            self.assertEqual(CRUMB, test_crumb)

    def test_get_dsn_host(self):
        """
        Test getting dsn just from host
        """
        conn = Connection(host="https://foo@sentry.io/123")
        dsn = get_dsn(conn)
        self.assertEqual(dsn, "https://foo@sentry.io/123")

    def test_get_dsn_env_var(self):
        """
        Test getting dsn from host, conn_type, login and schema
        """
        conn = Connection(conn_type="http", login="bar", host="getsentry.io", schema="987")
        dsn = get_dsn(conn)
        self.assertEqual(dsn, "http://bar@getsentry.io/987")

    def test_get_dsn_from_host_with_none(self):
        """
        Test getting dsn from host if other parameters are None
        """
        conn = Connection(conn_type="http", login=None, host="https://foo@sentry.io/123")
        dsn = get_dsn(conn)
        self.assertEqual(dsn, "https://foo@sentry.io/123")
