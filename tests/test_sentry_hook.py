import datetime
import unittest
from unittest.mock import Mock, MagicMock
from freezegun import freeze_time

from airflow.models import TaskInstance
from airflow.settings import Session
from airflow.utils import timezone

from sentry_sdk import configure_scope

from sentry_plugin.hooks.sentry_hooks import (
    SentryHook,
    add_sentry,
    get_task_instance_attr,
    new_clear_xcom,
)

EXECUTION_DATE = timezone.utcnow()
DAG_ID = "test_dag"
TASK_ID = "test_task"
OPERATOR = "test_operator"
STATE = "success"
TEST_SCOPE = {
    "dag_id": DAG_ID,
    "task_id": TASK_ID,
    "execution_date": EXECUTION_DATE,
    "ds": EXECUTION_DATE.strftime("%Y-%m-%d"),
    "operator": OPERATOR,
}
CRUMB_DATE = datetime.datetime(2019, 5, 15)
CRUMB = {
    "timestamp": CRUMB_DATE,
    "type": "default",
    "category": "data",
    "message": "Upstream Task: {}, State: {}, Operation: {}".format(
        TASK_ID, STATE, OPERATOR
    ),
    "level": "info",
}


class MockQuery(object):
    """
	Mock Query for qhen session is called.
	"""

    def __init__(self, task_instance):
        task_instance.state = STATE
        self.arr = [task_instance]

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return self.arr

    def delete(self):
        pass


class TestSentryHook(unittest.TestCase):
    def setUp(self):
        self.assertEqual(TaskInstance._sentry_integration_, True)
        self.sentry_hook = SentryHook
        self.dag = Mock(dag_id=DAG_ID)
        self.task = Mock(dag=self.dag, dag_id=DAG_ID, task_id=TASK_ID)
        self.task.__class__.__name__ = OPERATOR
        self.task.get_flat_relatives = MagicMock(return_value=[self.task])

        self.session = Session()
        self.ti = TaskInstance(self.task, execution_date=EXECUTION_DATE)
        self.session.query = MagicMock(return_value=MockQuery(self.ti))

    def test_add_sentry(self):
        """
        Test adding tags.
        """

        # Already setup TaskInstance
        with configure_scope() as scope:
            for key, value in scope._tags.items():
                self.assertEqual(TEST_SCOPE[key], value)

    def test_get_task_instance_attr(self):
        """
        Test getting object attributes.
        """

        state = get_task_instance_attr(self.ti, TASK_ID, "state", self.session)
        operator = get_task_instance_attr(self.ti, TASK_ID, "operator", self.session)
        self.assertEqual(state, STATE)
        self.assertEqual(operator, OPERATOR)

    @freeze_time(CRUMB_DATE.isoformat())
    def test_new_clear_xcom(self):
        """
        Test adding breadcrumbs.
        """

        new_clear_xcom(self.ti, self.session)
        self.task.get_flat_relatives.assert_called_once()

        with configure_scope() as scope:
            test_crumb = scope._breadcrumbs.pop()
            print(test_crumb)
            self.assertEqual(CRUMB, test_crumb)
