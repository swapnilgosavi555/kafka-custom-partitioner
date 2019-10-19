package com.knoldus

import java.util.Properties
import org.apache.kafka.clients.producer._

object KafkaProducer extends App {
  val props = new Properties()
  val topicName = "department"
  props.put("bootstrap.servers", "localhost:9092,localhost:9093")
  props.put(
    "key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  props.put(
    "value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  props.put("partitioner.class", "com.knoldus.CustomPartitioner")
  val producer = new KafkaProducer[String, String](props)
  try {
    for (i <- 0 to 5) {
      val record = new ProducerRecord[String, String](
        topicName,
        "IT",
        "My Site is knoldus.com " + i
      )
      producer.send(record)
    }
    for (i <- 0 to 5) {
      val record = new ProducerRecord[String, String](
        topicName,
        "COMP" + i,
        "My Site is knoldus.com " + i
      )
      producer.send(record)
    }

  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}
