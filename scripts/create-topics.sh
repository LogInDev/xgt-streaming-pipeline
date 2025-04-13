#!/bin/bash

kafka-topics.sh --create --topic raw_data \
  --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 || true

kafka-topics.sh --create --topic processed_data \
  --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 || true

kafka-topics.sh --list --bootstrap-server kafka:9092
