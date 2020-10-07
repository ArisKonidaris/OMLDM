package omldm.utils.kafkaPartitioners

import java.util

import omldm.messages.HubMessage
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.{Cluster, PartitionInfo}

class HubMessagePartitioner extends Partitioner {

  private var part: Int = -1

  println("-------------> new HubMessagePartitioner")

  override def partition(topic: String, key: Any,
                         keyBytes: Array[Byte],
                         value: Any,
                         valueBytes: Array[Byte],
                         cluster: Cluster): Int = {

//  val partitions: util.List[PartitionInfo] = cluster.partitionsForTopic(topic)
  val numPartitions: Int = cluster.partitionsForTopic(topic).size
//  println("Partition: " + value.asInstanceOf[HubMessage].destinations.head.getNodeId % numPartitions)
  value.asInstanceOf[HubMessage].destinations.head.getNodeId % numPartitions
  if (part == numPartitions - 1) part = 0 else part += 1
  part
}

  override def close(): Unit = { }

  override def configure(map: util.Map[String, _]): Unit = { }
}
