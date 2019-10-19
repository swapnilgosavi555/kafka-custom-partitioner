package com.knoldus

import java.util
import java.util.Properties
import scala.jdk.CollectionConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer

object KafkaConsumer extends App {

  val props: Properties = new Properties()
  val topicName = "department"

  props.put("group.id", "test")
  props.put("bootstrap.servers", "localhost:9092,localhost:9093")
  props.put(
    "key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  props.put(
    "value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  val consumer = new KafkaConsumer(props)

  try {
    consumer.subscribe(util.Arrays.asList(topicName))
    while (true) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        println(
          "Topic: " + record.topic() +
            ",Key: " + record.key() +
            ",Value: " + record.value() +
            ", Offset: " + record.offset() +
            ", Partition: " + record.partition()
        )
      }
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    consumer.close()
  }
}
