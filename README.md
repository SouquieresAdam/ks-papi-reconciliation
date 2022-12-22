# ks-papi-reconciliation

This repository will demonstrate how to reconcile data from multiple data source using processor api of Kafka Streams.

It will also demonstrate how to do :

- Automated unit tests
- Automated Integration test using Test Containers




## Prerequisites to build this code base

- Have a docker environment (required to run the Integration-Test classes)
- To clone & execute "gradle publishToMavenLocal" this awesome confluent container module project for the TestContainer tool :
  https://github.com/testcontainers-all-things-kafka/cp-testcontainers


This project use some code from https://github.com/confluentinc/kafka-streams-examples/, the two files that i used mention that ownership and their attached Apache 2.0 License

ContainerTestUtils.java which is a simplification of io.confluent.examples.streams.IntegrationTestUtils.java
io.confluent.examples.streams.utils.KeyValueWIthTimestamp is a simple copy paste

This is a temporary workaround.