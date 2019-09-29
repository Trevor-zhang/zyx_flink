package source.outersource

import common.config.KafkaConfig
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


/**
  * 数据源连接器-----kafka
  */
object KafkaSource {
  def main(args: Array[String]): Unit = {
       val env = StreamExecutionEnvironment.getExecutionEnvironment
       //开启checkpoint
       env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE)
       //设置checkpoint目录
       env.setStateBackend(new FsStateBackend("file:\\bigdata\\工具\\zyx_flink\\flink-common\\src\\main\\scala\\checkpoints.txt"))
       //设置重启策略
       env.setRestartStrategy(RestartStrategies.
         fixedDelayRestart(5,    //5次尝试
         5000))           //每次重试间隔50s

        //设置processTime处理时间
       env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

       //配置kafka连接属性
       val kafkaConfig = KafkaConfig.apply("string")
       val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1,new SimpleStringSchema(),kafkaConfig._2)
         .setStartFromLatest()

       import org.apache.flink.api.scala._

       val stream = env
         //addSource连接kafka
         .addSource(kafkaConsumer)
         .map(new RichMapFunction[String,Int] {
           //简单逻辑 数字转int 再乘以5 输出
           override def map(value: String): Int = {
             Integer.valueOf(value)*5
           }
         })
        stream.print()
        env.execute()
  }

}
