#!/bin/bash

if [[ $# -ne 3 ]]; then
    echo "Supply 3 arguments:"
    echo "1) Path to file which content serves as input to Kafka"
    echo "2) Bootstrap server (usually localhost:9092)"
    echo "3) Topic to produce to"
else
    source envsetup.sh

    INPUT_FILE=$1
    BOOTSTRAP_SERVER=$2
    TOPIC=$3
    $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $BOOTSTRAP_SERVER --topic $TOPIC < $INPUT_FILE
fi
