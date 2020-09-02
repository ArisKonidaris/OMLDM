# Online_Machine_Learning_via_Flink

Distributed High Scale Online Machine Learning via Apache Flink

## Installing
Fist clone the following maven project:\
Control: https://github.com/ArisKonidaris/Control.git \
BipartiteTopology: https://github.com/ArisKonidaris/BipartiteTopology.git \
OnlineMachineLearning: https://github.com/ArisKonidaris/OnlineMachineLearning.git

Install the Maven projects in the aforementioned order:
```
$ mvn clean install
```

## Getting started
After the installation of the required Maven projects follow the instuctions below:

```
$ mvn package
$ kafka/bin/kafka-topics.sh --create --bootstrap-server ip:port --partitions 36 --replication-factor 4 --topic trainingData
$ kafka/bin/kafka-topics.sh --create --bootstrap-server ip:port --partitions 36 --replication-factor 4 --topic forecastingData
$ kafka/bin/kafka-topics.sh --create --bootstrap-server ip:port --partitions 1 --replication-factor 4 --topic requests
$ kafka/bin/kafka-topics.sh --create --bootstrap-server ip:port --partitions 1 --replication-factor 4 --topic responses
$ kafka/bin/kafka-topics.sh --create --bootstrap-server ip:port --partitions 36 --replication-factor 4 --topic predictions
$ kafka/bin/kafka-topics.sh --create --bootstrap-server ip:port --partitions 36 --replication-factor 4 --topic psMessages

$ flink/bin/flink run /path/to/Online_Machine_Learning_via_Flink/target/oml1.2-0.0.1-SNAPSHOT.jar \
--parallelism 16 \
--trainingDataTopic <training data topic name> \
--trainingDataAddr <training data BrokerList> \
--forecastingDataTopic <forecasting data topic name> \
--forecastingDataAddr <forecasting data BrokerList> \
--requestsTopic <requests data topic name> \
--requestsAddr <requests BrokerList> \
--responsesTopic <responses topic name> \
--responsesAddr <responses BrokerList> \
--predictionsTopic <predictions topic name> \
--predictionsAddr <predictions BrokerList> \
--psMessagesTopic <psMessages topic name> \
--psMessagesAddr <psMessages BrokerList>