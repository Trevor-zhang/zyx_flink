package source.innersource

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
  * socket数据源
  * 在linux环境下，执行nc -lk 9099启动端口，在cli输入数据，
  * Flink就可以接收端口中的数据
  */
object SocketSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val socketStream:DataStream[String] = env.socketTextStream("192.168.0.146",9099)
    socketStream.print()
    env.execute()
  }

}
