package com.atguigu.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

/**
 * @description:
 * @author: malichun
 * @time: 2020/9/26/0026 14:13
 *
 */
object KafkaProducerUtil {
    def main(args: Array[String]): Unit = {
        writeToKafka("first2")


    }

    def writeToKafka(topic:String):Unit={
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "www.bigdata02.com:9092,www.bigdata03.com:9092,www.bigdata04.com:9092")
        properties.setProperty("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer")
        properties.setProperty("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String,String](properties)

        //从文件读取数据,逐行写入kafka
        val bufferSource = Source.fromFile("E:\\gitdir\\learn_projects\\UserBehaviorAnalysis_Flink_wushengran\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")

        for(line <- bufferSource.getLines()){
            val record = new ProducerRecord[String,String](topic,line)
            producer.send(record)

        }
        producer.close()


    }

}
