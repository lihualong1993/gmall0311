package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.GmallConstants
import com.atguigu.gmall.realtime.bean.Startuplog
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    // 1 消费kafka
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)


//    inputDStream.foreachRDD{
//      rdd=>{
//        println(rdd.map(_.value()).collect().mkString("\n"))
//      }
//    }

//     2 数据流转换结构变成case class 补充两个时间段
    val startuplogDStream: DStream[Startuplog] = inputDStream.map {
      record =>
        val jsonStr = record.value()
        val startuplog: Startuplog = JSON.parseObject(jsonStr, classOf[Startuplog])
        val dateTimeStr = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startuplog.ts))
        val dateArr: Array[String] = dateTimeStr.split(" ")
        startuplog.logDate = dateArr(0)
        startuplog.logHour = dateArr(1)
        startuplog
    }
    startuplogDStream.cache()

    // 3 利用用户清单进行过滤去重，只保留清单中不存在的用户访问记录
    val filterDStream: DStream[Startuplog] = startuplogDStream.transform {
      rdd =>
        val jedis: Jedis = RedisUtil.getJedisClient //driver 按周期执行
      val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        val key = "dau:" + dateStr
        val dauMidSet: util.Set[String] = jedis.smembers(key)
        jedis.close()
        val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)
        println("过滤前：" + rdd.count())
        val filterRDD: RDD[Startuplog] = rdd.filter { startuplog =>
          //executor
          val dauMidSet = dauMidBC.value
          !dauMidSet.contains(startuplog.mid)

        }
        println("过滤后：" + filterRDD.count())
        filterRDD
    }

    //批次内进行去重：按照mid进行分组，每组去第一个值
    val groupByMidDStream: DStream[(String, Iterable[Startuplog])] = filterDStream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
    val distictDStream: DStream[Startuplog] = groupByMidDStream.flatMap {
      case (mid, startlogItr) => {
        startlogItr.toList.take(1)
      }
    }

    //保存今日访问过的用户（mid）清单 - Redis
    distictDStream.foreachRDD{rdd=>
      //driver
      rdd.foreachPartition{startuplogItr=>
        val jedis: Jedis = RedisUtil.getJedisClient
        for(startuplog <- startuplogItr) {
          val key = "dau:" + startuplog.logDate
          jedis.sadd(key,startuplog.mid)
          println(startuplog)
        }
        jedis.close()
      }
    }

    //把数据写入hbase+Phoenix
    distictDStream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0311_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
    }





    println("启动流程")
    ssc.start()
    ssc.awaitTermination()
  }

}
