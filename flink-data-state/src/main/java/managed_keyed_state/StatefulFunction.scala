package managed_keyed_state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object StatefulFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val inputStream:DataStream[(Int,Long)] = env.fromElements((2,21L),(4,1L),(5,4L))
    inputStream
      .keyBy(_._1)
      .flatMap{new RichFlatMapFunction[(Int,Long),(Int,Long,Long)] {
        private var leastValueState:ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          //创建ValueStateDescriptor,定义状态名称为leastValue,并指定数据类型
          val leastValueStateDescriptor = new ValueStateDescriptor [Long]("leastValue",classOf[Long])
          //通过getRuntimeContext.getState获取state
          leastValueState = getRuntimeContext.getState(leastValueStateDescriptor)
        }
        override def flatMap(in: (Int, Long), collector: Collector[(Int, Long, Long)]): Unit = {
          //通过value方法从leastValueState中获取最小值
          val  leastValue = leastValueState.value()
          //如果当前指标大于最小值，则直接输出数据元素和最小值
          if (in._2>leastValue){
            collector.collect(in._1,in._2,leastValue)
          }else{
            //如果当前指标小于最小值，则更新状态中的最小值
            leastValueState.update(in._2)
            //将当前数据中的指标作为最小值输出
            collector.collect((in._1,in._2,in._2))
          }
        }
      }}
      .print()

    env.execute()
  }
}
