package source.outersource

import common.config.KafkaConfig
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, FlinkKafkaProducer011}

object kafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val stream :DataStream[String] = env.fromElements("1","2","4","5")
    val kafkaConfig = KafkaConfig.apply("string")
    val producer =new FlinkKafkaProducer(kafkaConfig._1,new SimpleStringSchema(),kafkaConfig._2)

    val stream2 :DataStream[String] =stream.filter(new FilterFunction[String] {
      override def filter(t: String): Boolean = {
        if(t.equals("1"))  {
           true
        } else
         false
      }
    })
    stream.print("stream")
    stream2.print("stream2")
    stream2.addSink(producer)
    env.execute()
  }
}
