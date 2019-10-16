package single_datastream

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object aa {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val source = env.fromCollection(Stream("1","2"))
     source.filter(new FilterFunction[String] {
      override def filter(t: String): Boolean = {
        if (Integer.valueOf(t)>1){
          return true
        }
        return false
      }
    })
    source.print()
    env.execute()

  }
}
