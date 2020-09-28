package com.atguigu.networkflow_analysis

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 *
 * 需求:每隔 5 秒，输出最近 10 分钟内访问量最多的前 N 个 URL
 * Created by John.Ma on 2020/9/28 0028 23:02
 */
// 定义输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

// 定义窗口聚合结果样例类
case class PageViewCount(url: String, windowEnd: Long, count: Long)

object HotPagesNetworkFlow {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //转换成样例提取时间戳,类并生成waterMark
        //        val inputStream = env.readTextFile("D:\\fileImportant\\Learn_projects\\UserBehaviorAnalysis_Flink_wushengran\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
        val inputStream = env.socketTextStream("www.bigdata01.com", 4444)
        val dataStream = inputStream
            .map(data => {
                val arr = data.split(" ")
                //对事件时间进行转换得到时间戳
                val simpleDataFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val ts = simpleDataFormat.parse(arr(3)).getTime
                ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
                override def extractTimestamp(element: ApacheLogEvent): Long = element.timestamp
            })

        //进行开窗聚合,以及排序输出
        val aggStream = dataStream
            .filter(_.method == "GET")
            .keyBy(_.url)
            .timeWindow(Time.minutes(10), Time.seconds(5))
            .allowedLateness(Time.milliseconds(1)) //处理乱序数据
            .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
            .aggregate(new PageCountAgg(), new PageViewCountWindowResult())


        //排序输出
        val resultStream = aggStream
            .keyBy(_.windowEnd)
            .process(new TopNHotPages(3))

        dataStream.print("data")
        aggStream.print("agg")
        aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")
        resultStream.print()

        env.execute("hot pages job")
    }
}

// AggregateFunction<IN, ACC, OUT>
class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}


//trait WindowFunction[IN, OUT, KEY, W <: Window] extends Function with Serializable {
class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {


    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
        out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
    }
}

//public abstract class KeyedProcessFunction<K, I, O> extends AbstractRichFunction {
class TopNHotPages(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

    //解决刷屏的bug,乱序数据
    //!!!!!!!!!!!!!!!!
    //    lazy val pageViewCountListState:ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list",classOf[PageViewCount]))

    //public class MapStateDescriptor<UK, UV> extends StateDescriptor<MapState<UK, UV>, Map<UK, UV>> {
    lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-Map", classOf[String], classOf[Long]))

    override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
        //        pageViewCountListState.add(value)
        pageViewCountMapState.put(value.url, value.count)
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
        //另外注册一个定时器,1分钟之后触发,这时窗口已经彻底关闭,不再有聚合结果输出,可以清空状态
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 60000L)
    }

    //到点排序输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        /*   val iter = pageViewCountListState.get().iterator()

     val allPageViewCounts:ListBuffer[PageViewCount] = new ListBuffer()

     while(iter.hasNext){
         allPageViewCounts +=  iter.next()
     }
        */

        // 判断定时器触发时间,如果已经是窗口结束时间1分钟之后,那么直接清空状态
        if(timestamp == ctx.getCurrentKey + 60000L ){
            pageViewCountMapState.clear()
            return
        }

        val allPageViewCounts: ListBuffer[(String, Long)] = new ListBuffer()
        val iter = pageViewCountMapState.entries().iterator()
        while (iter.hasNext) {
            val entry = iter.next()
            allPageViewCounts += ((entry.getKey, entry.getValue))
        }

        //提前清空状态
        // pageViewCountListState.clear()


        //按照访问量排序并输出top n
        val sortedPageViewCounts = allPageViewCounts.sortWith(_._2 > _._2).take(n)

        // 将排名信息格式化成String，便于打印输出可视化展示
        val result: StringBuilder = new StringBuilder
        result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

        // 遍历结果列表中的每个ItemViewCount，输出到一行
        for (i <- sortedPageViewCounts.indices) {
            val currentItemViewCount = sortedPageViewCounts(i)
            result.append("NO").append(i + 1).append(": \t")
                .append("页面URL = ").append(currentItemViewCount._1).append("\t")
                .append("热门度 = ").append(currentItemViewCount._2).append("\n")
        }

        result.append("\n==================================\n\n")

        //        Thread.sleep(1000)
        out.collect(result.toString())

    }


}