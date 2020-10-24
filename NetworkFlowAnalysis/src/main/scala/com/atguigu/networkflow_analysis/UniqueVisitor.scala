package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Created by John.Ma on 2020/10/4 0004 20:03
 *
 * 网站总访问量
 * uv
 *
 * 日志格式:
 * 543462,1715,1464116,pv,1511658000
 * 662867,2244074,1575622,pv,1511658000
 * 561558,3611281,965809,pv,1511658000
 *
 */
//定义输出Uv统计样例类
case class UvCount(windEnd: Long, count: Long)

object UniqueVisitor {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //从文件中读取数据
        val resource = getClass.getResource("/UserBehavior.csv")
        val inputStream = env.readTextFile(resource.getPath)

        //转换成样例类类型,并提取时间戳和watermark
        val dataStream = inputStream
            .map(data => {
                val arr = data.split(",")
                UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        val uvStream = dataStream
            .filter(_.behavior == "pv")
            .timeWindowAll(Time.hours(1)) //直接不分组,基于DataStream开一小时滚动窗口
//            .aggregate()
            .apply(new UvCountResult())

        uvStream.print()

        env.execute("uv job")

    }
}

//自定义实现全窗口函数
//用一个set结构来保存所有的数据(userId),进行自动去重
//trait AllWindowFunction[IN, OUT, W <: Window] extends Function with Serializable {
class UvCountResult() extends  AllWindowFunction[UserBehavior,UvCount,TimeWindow]{

    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
        //定义一个Set
        var userIdSet = Set[Long]()

        //遍历窗口中的所有数据元素,把userId添加到set中,自动去重
        for( userBehavior <- input )
            userIdSet += userBehavior.userId

        //将set的size作为去重后的uv值输出
        out.collect(UvCount(window.getEnd,userIdSet.size))

    }


}
