package common.config

import java.util.Properties

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._

/**
  * 读取kafka配置文件
  */
object KafkaConfig {
  def apply(topicName: String)= {
      val applicationConfig = ConfigFactory.load("conf/kafkaConfig")
      val config = applicationConfig.getConfig("dev-kafka")
      val stringTopic: String = config.as[String]("string.topic")
      val jsonTopic: String = config.as[String]("json.topic")
      val kvTopic: String = config.as[String]("kv.topic")
      val kv1Topic: String = config.as[String]("kv1.topic")
      val groupId: String = config.as[String]("group.id")
      val bootstrapServers: String = config.as[String]("bootstrap.servers")

      val props = new Properties()
      props.setProperty("bootstrap.servers",bootstrapServers)
      props.setProperty("group.id",groupId)

      topicName match {
        case "string" =>
          (stringTopic,props)
        case "kv" =>
          (kvTopic, props)
        case "kv1" =>
          (kv1Topic, props)
        case "json" =>
          (jsonTopic, props)
      }

  }
}
