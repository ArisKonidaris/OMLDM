package omldm.utils.kafkaPartitioners

import java.util

import omldm.messages.HubMessage
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.{Cluster, PartitionInfo}

class HubMessagePartitioner extends Partitioner {

  override def partition(topic: String, key: Any,
                         keyBytes: Array[Byte],
                         value: Any,
                         valueBytes: Array[Byte],
                         cluster: Cluster): Int = {

  val partitions: util.List[PartitionInfo] = cluster.partitionsForTopic(topic)
  val numPartitions: Int = partitions.size
  value.asInstanceOf[HubMessage].destinations.head.getNodeId % numPartitions
}

  override def close(): Unit = { }

  override def configure(map: util.Map[String, _]): Unit = { }
}
