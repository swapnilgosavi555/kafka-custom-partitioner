package com.knoldus

import java.util
import org.apache.kafka.common.record.InvalidRecordException
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

class CustomPartitioner extends Partitioner {
  val departmentName = "IT"
  override def configure(configs: util.Map[String, _]): Unit = {}

  override def partition(topic: String,
                         key: Any,
                         keyBytes: Array[Byte],
                         value: Any,
                         valueBytes: Array[Byte],
                         cluster: Cluster): Int = {
    val partitions = cluster.partitionsForTopic(topic)
    val numPartitions = partitions.size
    val it = Math.abs(numPartitions * 0.4).asInstanceOf[Int]

    if ((keyBytes == null) || (!key.isInstanceOf[String]))
      throw new InvalidRecordException(
        "All messages must have department name as key"
      )

    if (key.asInstanceOf[String] == departmentName) {
      val p = Utils.toPositive(Utils.murmur2(valueBytes)) % it
      System.out.println("Key = " + key.toString + " Partition = " + p)
      p
    } else {
      val p = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - it) + it
      System.out.println("Key = " + key.toString + " Partition = " + p)
      p
    }
  }

  override def close(): Unit = {}
}
