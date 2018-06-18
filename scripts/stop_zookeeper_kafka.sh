#!/bin/bash

source envsetup.sh

$KAFKA_HOME/bin/kafka-server-stop.sh

while [[ $(lsof -i :9092) ]]; do
    sleep 1
done

echo "Kafka stopped"

$KAFKA_HOME/bin/zookeeper-server-stop.sh

while [[ $(lsof -i :2181) ]]; do
    sleep 1
done

echo "Zookeeper stopped"
