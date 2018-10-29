
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util

import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

object append_log_reader {

  def main(args: Array[String]): Unit = {


    /* val conf = new SparkConf().setAppName("stream").setMaster("local[2]")
    //val session = SparkSession.builder().appName("word_counter").master("local").getOrCreate()
    val ssc = new StreamingContext(conf, Seconds(3))

    val data = ssc.textFileStream("/Users/abhinavkumar/Documents/stream/")
    data.print()

    /* val res= data.foreachRDD(t=>
    {
      val test = t.map(line=> line.toString)
    }
    )*/



    // res.print()
    data.print()
    ssc.start()
    ssc.awaitTermination()
  }*/

    val properties = new Properties()

    properties.put("bootstrap.servers", "localhost:9092")
   // properties.put("group.id", "consumer-tutorial")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[StringDeserializer])

    val kafkaConsumer = new KafkaConsumer[String, String](properties)
    kafkaConsumer.subscribe("test")

    while (true) {
      val results = kafkaConsumer.poll(2000).asScala
      for ((topic, data) <- results) {
        // Do stuff
        println(data)
      }
    }
  }
}
