package org.myorg.quickstart

import java.util.Properties

import id.dei.PageViews
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object AvroProducer {

  def main(args: Array[String]): Unit = {
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("key.serializer", classOf[StringSerializer].getName)
    kafkaProps.setProperty("value.serializer", classOf[ByteArraySerializer].getName)

    val producer = new KafkaProducer[String, Array[Byte]](kafkaProps)

    val pageViews = PageViews.newBuilder().setPageid("page-id").setViewtime(1020).setUserid("1234").build()


    for (a <- 1 to 100) {
      val record = new ProducerRecord[String, Array[Byte]]("sensor2", "" + a, PageViews.getEncoder.encode(pageViews).array())
      try {
        println("sending")
        val metadata = producer.send(record).get()
        println("sent")
        println(metadata.offset())

      } catch {
        case x: Exception => {
          x.printStackTrace()
        }
        case _ => println("error")
      }
    }

  }
}
