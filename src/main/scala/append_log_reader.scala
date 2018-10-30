
import java.util.{Collections, Properties}

import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecords
object append_log_reader {

  def main(args: Array[String]) :Unit = {

    val properties = new Properties()

    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "consumer-tutorial")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[StringDeserializer])
    //properties.put("partition.assignment.strategy", "range")

    val kafkaConsumer = new KafkaConsumer(properties)
    kafkaConsumer.subscribe(Collections.singletonList("test2"))

    while (true) {
      val results: ConsumerRecords[String, String] = kafkaConsumer.poll(5000).asInstanceOf[ConsumerRecords[String, String]]

      //println(results.count())
     for(  record <- results )
        {
          if (record.value().split(" ").contains("time=0.095")) {
            println(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}")
            record.value().split(" ").map(x=> println(x))
          }
        }
    }

  }
}
