# Sentry Airflow Plugin

A plugin for airflow dags and tasks that sets up [Sentry](sentry.io) for error logging.  

## Setup

### Local

Create a plugin folder in your `AIRFLOW_HOME` directory if you do not have one yet.

```
$ mkdir path/to/airflow/plugins
```

Then clone this repository in there.

```
$ cd path/to/airflow/plugins
$ git clone git@github.com:getsentry/sentry-airflow.git
```

Make sure you have setup your `SENTRY_DSN` in your environment variables!

### Google Composer

Add this folder to your plugin directory

```
gcloud composer environments storage plugins import --environment ENVIRONMENT_NAME \
    --location LOCATION \
    --source PATH_TO_LOCAL_FILE \
    --destination PATH_IN_SUBFOLDER
```

(For more information checkout Google's [Docs](https://cloud.google.com/composer/docs/concepts/plugins#installing_a_plugin))

Either set an environment variable on [Google composer](https://cloud.google.com/composer/docs/how-to/managing/environment-variables) for your `SENTRY_DSN`.

Or in the airflow webserver UI, add a connection for `sentry_dsn`. Let connection type be `HTTP` and the host be the DSN value.