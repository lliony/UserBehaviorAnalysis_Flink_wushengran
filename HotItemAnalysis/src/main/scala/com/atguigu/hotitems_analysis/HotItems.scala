package com.atguigu.hotitems_analysis

import java.sql.Timestamp
import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @description:
 * @author: malichun
 * @time: 2020/9/22/0022 16:55
 *
 */
//定义输入数据样例类
//543462,1715,1464116,pv,1511658000
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //定义事件时间语义

        //从文件中读取数据,并转换成样例类,提取时间戳生成watermark
//        val inputStream = env.readTextFile("E:\\gitdir\\learn_projects\\UserBehaviorAnalysis_Flink_wushengran\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")

        //从kafka读取数据
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "www.bigdata02.com:9092,www.bigdata03.com:9092,www.bigdata04.com:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")

        val inputStream = env.addSource( new FlinkKafkaConsumer[String]("first2", new SimpleStringSchema(), properties) )

        val dataStream = inputStream
            .map(data => {
                val arr = data.split(",")
                UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        //得到窗口的聚合结果
        val aggStream = dataStream
            .filter(_.behavior == "pv")
            .keyBy("itemId") //根据商品ID分组
            .timeWindow(Time.hours(1), Time.minutes(5)) //设置滑动窗口进行统计
            //public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable {
            //trait WindowFunction[IN, OUT, KEY, W <: Window] extends Function with Serializable {
            .aggregate(new CountAgg(), new ItemViewWindowResult()) //windowedStream的方法,又返回一个dataStream

        val resultSteam = aggStream
            .keyBy("windowEnd") //按照窗口进行分组,收集当前窗口内商品count数据
            .process(new TopNHotItems(5)) // 自定义处理流程

        resultSteam.print("")

        env.execute("hot items")
    }
}

//自定义语句和函数AggregateFunction ,聚合状态就是当前商品的count值
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] { //public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable {

    override def createAccumulator(): Long = 0L

    // 每来一条数据调用一次add，count值加一
    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数
class ItemViewWindowResult extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] { //trait WindowFunction[IN, OUT, KEY, W <: Window] extends Function with Serializable {

    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
        val itemId = key.asInstanceOf[Tuple1[Long]].f0
        val windowEnd = window.getEnd
        val count = input.iterator.next()
        out.collect(ItemViewCount(itemId, windowEnd, count))
    }
}

class TopNHotItems(topN:Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] { //public abstract class KeyedProcessFunction<K, I, O> extends AbstractRichFunction {
    //先定义状态:ListState
    var itemViewCountListState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
        itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor("itemViewCount", classOf[ItemViewCount]))
    }

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
        //每来一条数据,直接加入ListState
        itemViewCountListState.add(value)
        //注册一个windowEnd + 1 之后出发的定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

    }

    //定时器触发,可以认为所有窗口统计结果都已到齐,可以排序输出了
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        // 为了方便排序，另外定义一个ListBuffer，保存ListState里面的所有数据
        val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
        val iter = itemViewCountListState.get().iterator()
        while(iter.hasNext){
            allItemViewCounts += iter.next()
        }

        // 清空状态
        itemViewCountListState.clear()

        //按照count大小排序,取前n个
        val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topN)

        // 将排名信息格式化成String，便于打印输出可视化展示
        val result: StringBuilder = new StringBuilder
        result.append("窗口结束时间：").append( new Timestamp(timestamp - 1) ).append("\n")

        // 遍历结果列表中的每个ItemViewCount，输出到一行
        for( i <- sortedItemViewCounts.indices ){
            val currentItemViewCount = sortedItemViewCounts(i)
            result.append("NO").append(i + 1).append(": \t")
                .append("商品ID = ").append(currentItemViewCount.itemId).append("\t")
                .append("热门度 = ").append(currentItemViewCount.count).append("\n")
        }

        result.append("\n==================================\n\n")

//        Thread.sleep(1000)
        out.collect(result.toString())

    }
}