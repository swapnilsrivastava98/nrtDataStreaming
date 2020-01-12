#!/bin/bash

file="/home/uma/config.properties"

. /home/uma/config.properties 

#entering into kafka_2.12-2.2.0 folder
cd kafka_2.12-2.2.0

#checking the list of topics present
bin/kafka-topics.sh --list --zookeeper localhost:2181
 
#Creation of a topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $topicName

#checking the list of topics present after creating a new topic
bin/kafka-topics.sh --list --zookeeper localhost:2181



