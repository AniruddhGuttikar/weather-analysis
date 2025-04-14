# #!/bin/bash

# # Topic 1: Raw weather data
# kafka-topics.sh --create --topic weather-data \
#     --bootstrap-server localhost:9092 \
#     --partitions 1 --replication-factor 1

# # Topic 2: Humidity focused (optional processing)
# kafka-topics.sh --create --topic weather-humidity \
#     --bootstrap-server localhost:9092 \
#     --partitions 1 --replication-factor 1

# # Topic 3: Weather alerts (optional)
# kafka-topics.sh --create --topic weather-alerts \
#     --bootstrap-server localhost:9092 \
#     --partitions 1 --replication-factor 1

# echo "All Kafka topics created."


#!/bin/bash

docker exec kafka /usr/bin/kafka-topics --create --topic weather-data \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

docker exec kafka /usr/bin/kafka-topics --create --topic weather-humidity \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

docker exec kafka /usr/bin/kafka-topics --create --topic weather-alerts \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

echo "All Kafka topics created."
