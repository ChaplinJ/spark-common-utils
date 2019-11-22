package cn.ac.gcp

import java.sql.{Connection, ResultSet}

import cn.ac.gcp.utils.{QueryCallback, SqlParser, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

import scala.collection.mutable


/**
  * @Description kafka 0.10 获取Mysql的工具包
  *             该主构造方法当中传入的sql语句返回字段，必须包含传入的各个字段的值，否则将会报错
  *             @method saveOffsets 保存偏移量做了简单的实现，只会对传入的各个主要字段根据你表中的主键进行更新/插入操作，
  *                    如果需要更多的实现，需要自定义手动保存offset的操作
  * @Author chaplinj
  * @Date 2019/10/25 15:27
  * @EMAIL john_cc_zongheng@126.
  */
/**
  *
  * @param con
  * @param sql
  * @param groupColumn         存储消费者组的名字
  * @param topicColumn          topic的名字
  * @param partitionColumnName 存储分区号的字段名
  * @param offsetColumnName    存储偏移量的字段名
  */
class MySqlKafkaUtils(con: Connection, sql: String,
                      groupColumn: String, topicColumn: String, topics: Array[String],
                      partitionColumnName: String, offsetColumnName: String, kafkaParams: Map[String, Object]) extends Serializable {

    val result = new mutable.HashMap[TopicPartition, Long]
    /**
      * 获取偏移量
      *
      * @return
      */
    def fromOffsets() = {
        val proxy: SqlProxy = new SqlProxy
        proxy.executeQuery(con, sql, new QueryCallback {
            override def process(rs: ResultSet): Unit = {
                while (rs.next()) {
                    result.put(new TopicPartition(rs.getString(topicColumn.replaceAll("`","")),
                        rs.getInt(partitionColumnName.replaceAll("`",""))),
                        rs.getLong(offsetColumnName.replaceAll("`","")))
                }
            }
        })
        result
    }

    /**
      * 根据偏移量信息获取kafka输入流信息
      * @param ssc
      * @return
      */
    def getDirectStream(ssc: StreamingContext) = {
        val offsets = fromOffsets
        var result: InputDStream[ConsumerRecord[String, String]] = if (offsets.isEmpty) {
            KafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe(topics, kafkaParams))
        } else {
            KafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe(topics, kafkaParams, offsets))
        }
        result
    }

    /**
      * 保存数据库相关的表数据,
      *
      * @param stream            流
      */
    def saveOffsets(stream: InputDStream[ConsumerRecord[String, String]],groupId:String) = {
        stream.foreachRDD(rdd => {
            val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            val proxy: SqlProxy = new SqlProxy
            for (elem <- ranges) {
                val sb: StringBuilder = new StringBuilder
                sb.append("replace into ")
                    .append(SqlParser.getTableName(sql)).append(" (")
                    .append(groupColumn).append(",")
                    .append(topicColumn).append(",")
                    .append(partitionColumnName).append(",")
                    .append(offsetColumnName).append(") ")
                    .append("values(?,?,?,?)")
                println(sb.toString())
                proxy.executeUpdate(con, sb.toString(),Array(groupId,elem.topic,elem.partition,elem.fromOffset))
            }
        })
    }


}
