package function

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * ProcessWindowsFunction
  * 效率低，整个窗口的数据都会被传递给算子
  */
object ProcessWindowsFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.addSource(new SourceFunction[String] {
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (true){
          ctx.collect("hello")
          Thread.sleep(10000)
        }
      }

      override def cancel(): Unit = {}
    }).flatMap(_.split(""))
      .map((_,1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10000),Time.seconds(10)) //每隔 10s统计过去10s的数量和
      .process(new MyProcessWindowFunction)
      .print()
      .setParallelism(1)

      env.execute("word count")
  }

  class MyProcessWindowFunction extends ProcessWindowFunction[(String,Int),(String,Int),String,TimeWindow]{
    override def process(
                          key: String,
                          context: Context,
                          elements: Iterable[(String, Int)],
                          out: Collector[(String, Int)]): Unit = {
              var value = 0 ;
              elements.foreach(kv =>{
                value = value + kv._2
              })
      out.collect(key,value)

    }
  }
}
