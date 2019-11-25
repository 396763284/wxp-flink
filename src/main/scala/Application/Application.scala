package Application

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


/**
  * 1.kafka - 2.flink - 3.sink to es
  * flink 数据清洗，数据统计 关联mysql
  */
object Application {

  val logger = LoggerFactory.getLogger("Application")

  val MINITE_FORMAT = "yyyy-MM-dd HH:mm"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "kafka.bootstrap.servers" -> "localhost:9092",
      "kafka.group.id" -> "wxpgroup" ,//
      "kafka.topic" -> "wxpflink",
      "zk.host" -> "localhost:2181"
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 接收Kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",config("kafka.bootstrap.servers"))
    properties.setProperty("group.id",config("kafka.group.id"))
    properties.setProperty("zookeeper.connect", config("zk.host"))



    val consumer= new FlinkKafkaConsumer[String](config("kafka.topic"),new SimpleStringSchema(),properties)
    val stream = env.addSource(consumer)
    val soureFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val minFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val inputMap = stream.map(item=>{
      val line = item.split("\t")
      var timer = 0l
      try{
        timer = soureFormat.parse(line(3)).getTime
      }catch {
        case e:Exception=>{
          logger.error(s"time parse error:$line(3)",e.getMessage)
        }
      }
      (line(2),timer,line(5),line(6).toLong)
    }).filter(_._2!=0).filter(_._1 == "E")
    .map(x=>{
      (x._2,x._3,x._4)
    })
    inputMap.print().setParallelism(1)

    val result =  inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {
      val maxOutOfOrderness = 3500L // 3.5 seconds
      var currentMaxTimestamp: Long = _
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }
      override def extractTimestamp(element: (Long, String, Long), l: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1) // 按照 域名 keyby
        .window(TumblingEventTimeWindows.of(Time.seconds(60)))
        .apply(new WindowFunction[(Long,String,Long),(String,String,Long),Tuple,TimeWindow] {
          override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {
            /**
              * 1. 分钟时间  2019-11-11 20:10
              * 2. 域名
              * 3. traffic 的 和
              */
            val domain = key.getField(0).toString
            var sum = 0l
            val iterator= input.iterator
            while (iterator.hasNext){
              val next = iterator.next()
              sum +=next._3
            }
            println(key)
            println("key:"+key.getField(1))
            out.collect(("111",domain,sum))
          }
        })
    result.print().setParallelism(1)
    env.execute("Application")

  }

  def tranTimeToString(tm:Long) :String={
    val fm = new SimpleDateFormat(MINITE_FORMAT)
    val tim = fm.format(new Date(tm))
    tim
  }

}
