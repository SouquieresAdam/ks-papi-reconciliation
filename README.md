# ks-papi-reconciliation

This repository will demonstrate how to reconcile data from multiple data source using various operators of Kafka Streams.

Two different methods were used to achieve the same result :

- A processor API oriented one for maximum flexibility
- A Stream API oriented one using cogroup & aggregate operators for less line of codes and less technicalities

Some unit tests will demonstrate that both topologies achieve the same result.


## Diagram

Here is a schematic representation of the target topology (with Processor API) :

![Visual step-by-step representation of the target topology](diagram.png)
