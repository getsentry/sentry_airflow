[![Build Status](https://travis-ci.com/getsentry/sentry_airflow.svg?branch=master)](https://travis-ci.com/getsentry/sentry_airflow)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Sentry Status](https://img.shields.io/badge/sentry-sign_up-white.svg?logo=sentry&style=social)](https://docs.sentry.io)

# Sentry Airflow Plugin

A plugin for [Airflow](https://airflow.apache.org/) dags and tasks that sets up [Sentry](https://sentry.io) for error logging.  

## Setup

### Local

Install the `sentry-sdk` with the `flask` extra:

```shell
$ pip install --upgrade 'sentry-sdk[flask]==0.11.2'
```

Create a plugin folder in your `AIRFLOW_HOME` directory if you do not have one yet:

```shell
$ mkdir -p $AIRFLOW_HOME/plugins
```

Then clone this repository in there:

```shell
$ cd $AIRFLOW_HOME/plugins
$ git clone git@github.com:getsentry/sentry_airflow.git
```

**Make sure you have setup your `SENTRY_DSN` in your environment variables!** The DSN can be found in Sentry by navigating to [Project Name] -> Project Settings -> Client Keys (DSN). Its template resembles the following: `'{PROTOCOL}://{PUBLIC_KEY}@{HOST}/{PROJECT_ID}'`

### Google Composer

Install the `sentry-sdk` into Google Composer's [Python dependencies](https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies#install-package).

Add this folder to your plugin directory:

```shell
$ gcloud composer environments storage plugins import --environment ENVIRONMENT_NAME \
    --location LOCATION \
    --source PATH_TO_LOCAL_FILE \
    --destination PATH_IN_SUBFOLDER
```

(For more information checkout Google's [Docs](https://cloud.google.com/composer/docs/concepts/plugins#installing_a_plugin))

Either set an environment variable on [Google composer](https://cloud.google.com/composer/docs/how-to/managing/environment-variables) for your `SENTRY_DSN`
or in the Airflow webserver UI, add a connection (Admin->Connections) for `sentry_dsn`. Let the connection type be `HTTP` and the host be the DSN value.
