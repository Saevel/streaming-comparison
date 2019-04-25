TODO: NEXT

SCENARIO:

Part 1: Retrieve OriginalUser from Kafka, deserialize and transform to User, save to Kafka

Part 2: Retrieve Users, Accounts and Transactions from Kafka topic, join and save joined structure to another Kafka topic

Part 3: For joined Users, Accounts and Transactions, check the correctness of the balance and save results to another Kafka topic


IMPLEMENTATIONS: 

- Spark Structured Streaming

- Flink Streaming Scala

- Kafka Streams Scala

- Akka Streams Kafka


TESTS:

 - Generic Integration tests for all three cases
 
 - Gatling tests for each streaming scenario

PROVISIONING:

- S3 bucket for Terraform backend

- Spark cluster on AWS EMR

- FLink cluster on AWS EMR

- Kafka Streams on AWS ECS

- Akka Steams on AWS ECS

- Gatling on AWS ECS