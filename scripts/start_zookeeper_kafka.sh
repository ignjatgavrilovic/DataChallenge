#!/bin/bash

source envsetup.sh

$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &>/dev/null &

while [[ -z $(lsof -i :2181) ]]
do
    echo "Waiting for Zookeeper..."
    sleep 1
done

$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &>/dev/null &

while [[ -z $(lsof -i :9092) ]]
do
    echo "Waiting for Kafka..."
    sleep 1
done

if [[ $(lsof -i :2181) ]]; then
    echo "Zookeeper is up and running"
fi

if [[ $(lsof -i :9092) ]]; then
    echo "Kafka is up and running"
fi


    
