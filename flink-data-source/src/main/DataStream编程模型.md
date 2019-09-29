  在flink整个架构中。对流计算的支持是其最重要的功能之一，Flink基于Google提出的
  DataFlow模型(https://cloud.google.com/dataflow)，实现了支持原生数据流处理的计算引擎。
 
  Dataflow的核心就是窗口和触发模型，而Flink在这两方面的实现，最接近Dataflow的理论原型，事件时间驱动，各种窗口模型，自定义触发和乱序／晚到数据的处理等等。
  
  Flink的Data Streaming API通过定义window方法，和window内的数据需要使用的聚合函数比如：reduce，fold，window（前两者增量，后者全量），以及窗口触发（Trigger）和窗口内数据的淘汰（Evictor）方法，让用户可以实现对Dataflow模型中定义的场景的灵活处置，比如：需要在大数据量，大窗口尺度内实现实时连续输出结果的目的。通过allow late数据的时间范围来处理晚到数据。
  不过晚到数据会触发聚合结果的再次输出，这个和Dataflow的模型不同的是，Flink本身是不提供反向信息输出的，需要业务逻辑自行做必要的去重处理。
  对于Flink的实现，个人比较赞同的一点，是对数据的聚合和淘汰方式，给用户留下了足够灵活的选择，毕竟在工程实践中，长时间，大窗口，连续结果输出这种场景很常见，比如实时统计一天之类各个小时段的PV／UV，5秒更新一次结果。这种情况下，要避免OOM，还要正确处理晚到数据，追数据等问题，预聚合和提前触发的能力就必不可少了。
  整体感觉Flink目前在Dataflow模型思想方面实现的成熟度比Spark Structured Streaming要好。

  Flink中定义了DataStream API让用户灵活高效地编写Flink流式应用。DataStream API主要可分为三个部分，DataSource模块，Transformation模块以及DataSink模块，
  其中Sources模块主要定义了数据接入功能，主要是将各种外部数据接入至Flink系统中，并将接入数据转换成DataStream数据集。在Transformation模块定义了对DataStream
  数据集的各种转换操作，例如进行map、filter、windows等操作。最后，将结果数据通过DataSink模块写出到外部存储介质中，例如将数据输出到文件或Kafka消息中间件等。
  