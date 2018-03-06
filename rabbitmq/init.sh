#!/bin/bash
set -e

echo Starting RabbitMQ server...
rabbitmq-server --hostname localhost &> /dev/null &

echo Waiting for the server to get ready...
for i in {30..0}; do
        if rabbitmqctl -t 1 list_queues &> /dev/null; then
                break
        fi
        echo 'RabbitMQ is not ready yet...'
        sleep 1
done
if [ "$i" = 0 ]; then
        echo >&2 'RabbitMQ init process failed.'
        exit 1
fi

echo Adding user...
rabbitmqctl add_user $RABBITMQ_USER $RABBITMQ_PASS
rabbitmqctl set_user_tags $RABBITMQ_USER administrator
rabbitmqctl set_permissions -p / $RABBITMQ_USER  ".*" ".*" ".*"

echo 'RabbitMQ init process done.'
