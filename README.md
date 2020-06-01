# kafka-test

FIXME: description

## Installation

Install kafka
setup server1.properties in kafka config `listeners=PLAINTEXT://:9092` 
## Usage

start zookeeper -> $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_CONFIG/zookeeper.properties
start  kafka -> $KAFKA_HOME/bin/kafka-server-start.sh config/server1.properties 

push data to kafka -> lein run produce --file MovieSummaries/plot_summaries-1.txt
consume data -> lein run consume

 
