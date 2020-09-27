package com.atguigu.hotitems_analysis

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.types.Row

/**
 * @description:
 * @author: malichun
 * @time: 2020/9/26/0026 14:28
 *
 */
object HotItemsWithSql {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val inputStream = env.readTextFile("E:\\gitdir\\learn_projects\\UserBehaviorAnalysis_Flink_wushengran\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")


        val dataStream = inputStream
            .map(data => {
                val arr = data.split(",")
                UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        //定义表执行环境
        val settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .useBlinkPlanner()
            .build()

        val tableEnv = StreamTableEnvironment.create(env,settings)

        //基于DataSteam创建Table
        val dataTable = tableEnv.fromDataStream(dataStream,'itemId, 'behavior , 'timestamp.rowtime as 'ts)

        //1. Table API进行开窗聚合统计
        val aggTable = dataTable
            .filter('behavior === "pv")
            .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
            .groupBy('itemId, 'sw)
            .select('itemId, 'sw.end  as 'windowEnd ,'itemId.count as 'cnt)

        // 用SQL去实现TopN的选取
        tableEnv.createTemporaryView("aggTable",aggTable,'itemId, 'windowEnd,'cnt)
        val resultTable = tableEnv.sqlQuery(
            """
              |select
              |     *
              |from
              |(
              |select
              |     *,
              |     row_number() over (partition by windowEnd order by cnt desc) as row_num
              |from
              |     aggTable
              |) t
              |where row_num <= 5
              |""".stripMargin)

        resultTable.toRetractStream[Row].print()


        //纯SQL实现
        tableEnv.createTemporaryView("dataTable",dataStream,'itemId,'behavior,'timestamp.rowtime as 'ts)
        val resultSqlTable = tableEnv.sqlQuery(
            """
              |select
              |     *
              |from
              |(
              |select
              |     *,
              |     row_number() over (partition by windowEnd order by cnt desc) as row_num
              |from
              |     (
              |         select
              |             itemId,
              |             hop_end(ts,interval '5' minute,interval '1' hour) as windowEnd,  -- hop,滑动跳动的意思,如果是滚动用tumble(ts,interval '1' hour )
              |             count(itemId) as cnt
              |         from
              |             dataTable
              |         where behavior = 'pv'
              |         group by
              |             itemId,
              |             hop(ts,interval '5' minute,interval '1' hour) -- hop(字段,滑动,窗口大小)  --滚动 tumble(ts, interval '1' hour)
              |     ) t
              |) t
              |where row_num <= 5
              |
              |""".stripMargin)
        resultSqlTable.toRetractStream[Row].print()


        env.execute()

    }
}
