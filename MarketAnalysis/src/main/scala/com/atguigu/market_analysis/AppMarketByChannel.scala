package com.atguigu.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @description: 分渠道统计
 * @author: malichun
 * @time: 2020/10/14/0014 13:39
 *
 */
//定义输入数据的样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp:Long)

//定义输出数据样例类
case class MarketViewCount(windowStart:String , windowEnd:String, channel:String, behavior:String, count:Long)

//自定义测试数据源
class SimulatedSource() extends RichSourceFunction[MarketUserBehavior]{
    //是否运行的标识位
    var running = true

    //定义用户行为和渠道的集合
    val behaviorSet :Seq[String] = Seq("view", "download","install","uninstall")
    val channelSet:Seq[String] = Seq("appstore","weibo","wechat","tieba")
    val rand:Random = Random

    override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
        // 测试的数据量的上限
        //定义一个生成数据最大的数量
        val maxCounts = Long.MaxValue
        var count = 0L

        //while循环,随机地产生数据
        while(running && count < maxCounts){
            val id = UUID.randomUUID().toString
            val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
            val channel = channelSet(rand.nextInt(channelSet.size))

            val ts = System.currentTimeMillis()

            ctx.collect(MarketUserBehavior(id,behavior,channel, ts))

            count += 1
            Thread.sleep(50L)

        }



    }

    //停止生成数据
    override def cancel(): Unit = {
        running = false
    }
}

object AppMarketByChannel {
    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val dataStream = env.addSource(new SimulatedSource)
            .assignAscendingTimestamps(_.timestamp)  //升序,毫秒

        // 开窗统计输出
        val resultStream = dataStream
            .filter(_.behavior != "uninstall")
            .keyBy( d => (d.channel,d.behavior))  //根据2个key,每个渠道,每个行为
            .timeWindow(Time.days(1),Time.seconds(5)) // 1天的数据,5秒钟输出一次
//            .aggregate(new )
            .process(new MarketCountByChannel())


        resultStream.print()

        env.execute("app market by channel job")

    }
}

//自定义ProcessWindowFunction
//abstract class ProcessWindowFunction[IN, OUT, KEY, W <: Window] extends AbstractRichFunction {
class MarketCountByChannel() extends  ProcessWindowFunction[MarketUserBehavior,MarketViewCount,(String,String),TimeWindow]{

    //所有数据都搜集齐了,当前窗口要触发计算的时候,调用precess方法
    override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {

        val start = new Timestamp(context.window.getStart).toString
        val end = new Timestamp(context.window.getEnd).toString
        val channel = key._1
        val behavior = key._2

        val count = elements.size
        out.collect(MarketViewCount(start,end,channel,behavior,count))
    }


}
