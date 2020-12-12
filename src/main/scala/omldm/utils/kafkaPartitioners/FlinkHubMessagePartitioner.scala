package omldm.utils.kafkaPartitioners

import omldm.messages.HubMessage
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner

class FlinkHubMessagePartitioner extends FlinkKafkaPartitioner[HubMessage] {
  override def partition(record: HubMessage,
                         key: Array[Byte],
                         value: Array[Byte],
                         targetTopic: String,
                         partitions: Array[Int]): Int = {
    if (record.destinations.length == 1)
      record.destinations.head.getNodeId % partitions.length
    else
      record.networkId % partitions.length
  }
}
