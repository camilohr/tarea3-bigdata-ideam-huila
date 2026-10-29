#!/usr/bin/env bash
# Inicia ZooKeeper y Kafka (si no estan corriendo) y crea el topic si no existe.
# Uso:  bash iniciar_kafka.sh
KAFKA=/opt/Kafka
TOPIC=ideam_temperatura

if ! jps | grep -q QuorumPeerMain; then
  echo "Iniciando ZooKeeper..."
  nohup $KAFKA/bin/zookeeper-server-start.sh $KAFKA/config/zookeeper.properties > ~/zookeeper.log 2>&1 &
  sleep 10
fi

if ! jps | grep -qw Kafka; then
  echo "Iniciando Kafka..."
  export KAFKA_HEAP_OPTS="-Xms256m -Xmx512m"
  nohup $KAFKA/bin/kafka-server-start.sh $KAFKA/config/server.properties > ~/kafka.log 2>&1 &
fi

echo "Esperando al broker..."
for i in $(seq 1 20); do
  $KAFKA/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1 && break
  sleep 3
done

if ! $KAFKA/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx $TOPIC; then
  echo "Creando topic $TOPIC..."
  $KAFKA/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic $TOPIC
fi

echo
jps
$KAFKA/bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic $TOPIC
