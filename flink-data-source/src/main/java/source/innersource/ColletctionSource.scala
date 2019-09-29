package source.innersource

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * 集合数据源
  */
object ColletctionSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    //(0)用element创建DataSteam
    val ds0:DataStream[String]=env.fromElements("flink1.7","flink1.8","flink1.9")
    ds0.print()

    //(1)用Tuple创建DataStream
    val ds1:DataStream[(Int,String)] = env.fromElements((1,"flink1.7"),(2,"flink1.8"),(3,"flink1.9"))
    ds1.print()

    //(2)用Array创建DataStream
    val ds2:DataStream[String] = env.fromCollection(Array("flink1.7","flink1.8"))
    ds2.print()

    //(3)用List创建DataStream
    val ds3:DataStream[String] = env.fromCollection(List("flink1.7","flink1.8"))
    ds3.print()

    //(4)用stream创建DataStream  Stream相当于lazy List，避免在中间过程中生成不必要的集合
    val ds4:DataStream[String] = env.fromCollection(Stream("flink1.7","flink1.8"))
    ds4.print()


    env.execute()
  }
}
