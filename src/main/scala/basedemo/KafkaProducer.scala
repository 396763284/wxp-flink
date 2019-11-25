package basedemo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    val config = Map(
      "kafka.bootstrap.servers" -> "localhost:9092",
      "kafka.group.id" -> "wxpgroup" ,//
      "kafka.topic" -> "wxpflink",
      "zk.host" -> "localhost:2181"
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment





  }

}
