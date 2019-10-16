package function


import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Reducefunction
  * 定义了对输入的两个相同类型的数据元素按照指定的计算方法进行聚合和逻辑
  * 输出类型相同的一个结果元素
  *  简单易用，推荐使用
  */
object ReduceFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.addSource(new SourceFunction[String] {
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (true){
          ctx.collect("hello")
          Thread.sleep(5000)
        }
      }

      override def cancel(): Unit = {}
    }).flatMap(_.split(""))
      .map((_,1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10)) //每隔 10s统计过去10s的数量和
      .reduce((x,y) =>{
      (x._1,x._2+y._2)
       })
      .addSink(x => {
      print(x+"\n")
    })

    env.execute("word count")

  }

}
