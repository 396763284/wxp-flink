package basedemo

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object kafkaConsumer {

  def main(args: Array[String]): Unit = {
    val config = Map(
      "kafka.bootstrap.servers" -> "localhost:9092",
      "kafka.group.id" -> "wxpgroup" ,//
      "kafka.topic" -> "wxpflink",
      "zk.host" -> "localhost:2181"
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收Kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",config("kafka.bootstrap.servers"))
    properties.setProperty("group.id",config("kafka.group.id"))
    properties.setProperty("zookeeper.connect", config("zk.host"))






  }
}
