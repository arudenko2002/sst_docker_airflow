#!/usr/bin/env bash

AIRFLOW_HOME="/usr/local/airflow"
CMD="airflow"
TRY_LOOP="20"

: ${REDIS_HOST:="redis"}
: ${REDIS_PORT:="6379"}
: ${REDIS_PASSWORD:=""}

: ${POSTGRES_HOST:="postgres"}
: ${POSTGRES_PORT:="5432"}
: ${POSTGRES_USER:="airflow"}
: ${POSTGRES_PASSWORD:="airflow"}
: ${POSTGRES_DB:="airflow"}


: ${MYSQL_HOST:="127.0.0.1"}
: ${MYSQL_PORT:="3306"}
: ${MYSQL_USER:="airflow"}
: ${MYSQL_PASSWORD:="airflow"}
: ${MYSQL_DB:="airflow"}

: ${CLOUD_SQL_INSTANCE_PORT:="3306"}

: ${IS_CLOUD_SQL:="N"}

: ${IS_MYSQL:="N"}

: ${IS_EMAIL:="N"}
: ${SMTP_HOST:="localhost"}
: ${SMPT_PORT:="25"}
: ${SMTP_IP:="127.0.0.1"}
: ${SMTP_MAIL_FROM:="airflow@airflow.com"}


: ${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}

# Load DAGs exemples (default: Yes)
if [ "$LOAD_EX" = "n" ]; then
    sed -i "s/load_examples = True/load_examples = False/" "$AIRFLOW_HOME"/airflow.cfg
fi

# kickstart cloud sql proxy in  (default: Yes)
if [ "$IS_CLOUD_SQL" = "Y" ]; then
 nohup /usr/local/cloud_sql_proxy -instances=$CLOUD_SQL_INSTANCE_NAME=tcp:$CLOUD_SQL_INSTANCE_PORT &
fi

# Install custome python package if requirements.txt is present
if [ -e "/requirements.txt" ]; then
    $(which pip) install --user -r /requirements.txt
fi

# Update airflow config - Fernet key
sed -i "s|\$FERNET_KEY|$FERNET_KEY|" "$AIRFLOW_HOME"/airflow.cfg

# Email Server
if [ "$IS_EMAIL" = "Y" ]; then
  sed -i "s#smtp_host = localhost#smtp_host = $SMTP_HOST" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#smtp_port = 25#smtp_port = $SMTP_PORT" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#smtp_mail_from = airflow@airflow.com#smtp_port = $SMTP_MAIL_FROM" "$AIRFLOW_HOME"/airflow.cfg

  echo "$SMTP_IP\t$SMTP_HOST" >> /etc/hosts
fi




# Update google project name
sed -i "s|\$PROJECT_NAME|$PROJECT_NAME|" "$AIRFLOW_HOME"/airflow.cfg

if [ -n "$REDIS_PASSWORD" ]; then
    REDIS_PREFIX=:${REDIS_PASSWORD}@
else
    REDIS_PREFIX=
fi

# Wait for Postresql
if [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "scheduler" ] ; then
  i=0
  if [ "$IS_MYSQL" = "Y" ]; then
      while ! nc -z $MYSQL_HOST $MYSQL_PORT >/dev/null 2>&1 < /dev/null; do
        i=$((i+1))
        if [ "$1" = "webserver" ]; then
          echo "$(date) - waiting for ${MYSQL_HOST}:${MYSQL_PORT}... $i/$TRY_LOOP"
          if [ $i -ge $TRY_LOOP ]; then
            echo "$(date) - ${MYSQL_HOST}:${MYSQL_PORT} still not reachable, giving up"
            exit 1
          fi
        fi
        sleep 10
      done
  else
      while ! nc -z $POSTGRES_HOST $POSTGRES_PORT >/dev/null 2>&1 < /dev/null; do
        i=$((i+1))
        if [ "$1" = "webserver" ]; then
          echo "$(date) - waiting for ${POSTGRES_HOST}:${POSTGRES_PORT}... $i/$TRY_LOOP"
          if [ $i -ge $TRY_LOOP ]; then
            echo "$(date) - ${POSTGRES_HOST}:${POSTGRES_PORT} still not reachable, giving up"
            exit 1
          fi
        fi
        sleep 10
      done
  fi
fi

# Update configuration depending the type of Executor
if [ "$EXECUTOR" = "Celery" ]
then
  # Wait for Redis
  if [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "scheduler" ] || [ "$1" = "flower" ] ; then
    j=0
    while ! nc -z $REDIS_HOST $REDIS_PORT >/dev/null 2>&1 < /dev/null; do
      j=$((j+1))
      if [ $j -ge $TRY_LOOP ]; then
        echo "$(date) - $REDIS_HOST still not reachable, giving up"
        exit 1
      fi
      echo "$(date) - waiting for Redis... $j/$TRY_LOOP"
      sleep 5
    done
  fi
  if [ "$IS_MYSQL" = "Y" ]; then
    sed -i "s#sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow#sql_alchemy_conn = mysql+mysqldb://$MYSQL_USER:$MYSQL_PASSWORD@$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DB#" "$AIRFLOW_HOME"/airflow.cfg
    sed -i "s#celery_result_backend = db+postgresql://airflow:airflow@postgres/airflow#celery_result_backend = db+mysql://$MYSQL_USER:$MYSQL_PASSWORD@$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DB#" "$AIRFLOW_HOME"/airflow.cfg
  else
    sed -i "s#sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow#sql_alchemy_conn = postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB#" "$AIRFLOW_HOME"/airflow.cfg
    sed -i "s#celery_result_backend = db+postgresql://airflow:airflow@postgres/airflow#celery_result_backend = db+postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB#" "$AIRFLOW_HOME"/airflow.cfg
  fi

  sed -i "s#broker_url = redis://redis:6379/1#broker_url = redis://$REDIS_PREFIX$REDIS_HOST:$REDIS_PORT/1#" "$AIRFLOW_HOME"/airflow.cfg
  if [ "$1" = "webserver" ]; then
    echo "Initialize database..."
    $CMD initdb
    exec $CMD webserver
  else
    sleep 10
    exec $CMD "$@"
  fi
elif [ "$EXECUTOR" = "Local" ]
then
  sed -i "s/executor = CeleryExecutor/executor = LocalExecutor/" "$AIRFLOW_HOME"/airflow.cfg
  if [ "$IS_MYSQL" = "Y" ]; then
    sed -i "s#sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow#sql_alchemy_conn = mysql+mysqldb://$MYSQL_USER:$MYSQL_PASSWORD@$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DB#" "$AIRFLOW_HOME"/airflow.cfg
  else
    sed -i "s#sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow#sql_alchemy_conn = postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB#" "$AIRFLOW_HOME"/airflow.cfg
  fi

  sed -i "s#broker_url = redis://redis:6379/1#broker_url = redis://$REDIS_PREFIX$REDIS_HOST:$REDIS_PORT/1#" "$AIRFLOW_HOME"/airflow.cfg
  echo "Initialize database..."
  $CMD initdb
  exec $CMD webserver &
  exec $CMD scheduler
# By default we use SequentialExecutor
else
  if [ "$1" = "version" ]; then
    exec $CMD version
    exit
  fi
  sed -i "s/executor = CeleryExecutor/executor = SequentialExecutor/" "$AIRFLOW_HOME"/airflow.cfg
  sed -i "s#sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow#sql_alchemy_conn = sqlite:////usr/local/airflow/airflow.db#" "$AIRFLOW_HOME"/airflow.cfg
  echo "Initialize database..."
  $CMD initdb
  exec $CMD webserver
fi
