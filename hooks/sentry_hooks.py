from airflow.hooks.base_hook import BaseHook
from raven import Client
from raven.contrib.celery import register_signal, register_logger_signal
from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler 
import logging


class SentryHook(BaseHook):
	def __init__(self, sentry_conn_id = None):
		self.sentry_conn_id = sentry_conn_id
		self.connection = self.get_connection(sentry_conn_id)     
		self.dsn = self.sentry_conn_id.extra_dejson.get('dsn')

    def get_conn(self):
		@provide_session
		client = raven.Client()
		if self.dsn is not None:
			client = raven.Client(self.dsn)
		return client

    def set_signals(self):
    	client = self.get_conn()
    	register_logger_signal(client)
    	register_signal(client)

    def set_logger(self):
		handler = SentryHandler()
		if self.dsn is not None:
			handler = SentryHandler(dsn=self.dsn)
		handler.setLevel(logging.ERROR)
		setup_logging(handler)