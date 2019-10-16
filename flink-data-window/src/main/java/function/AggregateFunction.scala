package function

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * AggregateFunction
  * 与ReduceFunction类似，优点在于在窗口计算上更加通用,但实现更复杂
  */
object AggregateFunction {
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
        .print("count")
//      .keyBy(_._1)
//      .timeWindow(Time.seconds(10000),Time.seconds(10))
//      .aggregate(new myAggregateFunction)
//      .print()
//      .setParallelism(1)

    env.execute("word count")


  }

  /**
    * ACC createAccumulator(); 迭代状态的初始值
    * ACC add(IN value, ACC accumulator); 每一条输入数据，和迭代数据如何迭代
    * ACC merge(ACC a, ACC b); 多个分区的迭代数据如何合并
    * OUT getResult(ACC accumulator); 返回数据，对最终的迭代数据如何处理，并返回结果。
    */
  class myAggregateFunction extends AggregateFunction[(String,Int),(String,Int),(String,Int)]{
    override def createAccumulator(): (String, Int) = {
      ("",0)
    }

    override def add(in: (String, Int), acc: (String, Int)): (String, Int) = {
      (in._1,acc._2+in._2)
    }

    override def getResult(acc: (String, Int)): (String, Int) = acc

    override def merge(acc: (String, Int), acc1: (String, Int)): (String, Int) = {
      (acc._1,acc._2+acc1._2)
    }
  }
}
