package source.innersource

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * 文件数据源 resources/file.csv
  */
object FileSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //若读取不到文件用绝对路径
    val csv = env.readTextFile("file.csv")
    csv.print()
    env.execute();
  }

}
