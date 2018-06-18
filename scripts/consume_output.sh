#!/bin/bash

if [[ $# -ne 2 ]]; then
    echo "Supply 2 arguments:"
    echo "1) Bootstrap server (usually localhost:9092)"
    echo "2) Topic to consume"
else
    source envsetup.sh
    
    $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server $1 --topic $2
fi

