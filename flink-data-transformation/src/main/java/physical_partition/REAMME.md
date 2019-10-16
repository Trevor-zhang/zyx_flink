物理分区的作用是根据指定的分区策略将数据重新分配到不同节点的Task实例上执行。
Flink内部提供的常见数据分区策略如下
（1）随机分区
  val shuffleStream = dataStream.shuffle
（2）重平衡分区
  val shuffleStream = dataStream.rebalance()
（3）广播操作
  val shuffleStream = dataStream.broadcast()
 (4)自定义分区

