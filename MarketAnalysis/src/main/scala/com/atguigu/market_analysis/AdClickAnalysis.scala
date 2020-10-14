package com.atguigu.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @description: 页面广告点击量统计,按照省份
 * @author: malichun
 * @time: 2020/10/14/0014 14:33
 *
 *        日志格式:
 *        userId,广告id,  省,  城市,  ,时间戳
 *        543462,1715,beijing,beijing,1511658000
 *        662867,2244074,guangdong,guangzhou,1511658060
 *        561558,3611281,guangdong,shenzhen,1511658120
 *        894923,1715,beijing,beijing,1511658180
 *
 *
 */
//定义输入输出样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timeStamp: Long)

case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)
//侧输出流黑名单报警信息样例类
case class BlackListUserWarning(userId: Long, adId: Long, msg: String)

object AdClickAnalysis {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //从文件中读取数据
        val resource = getClass().getResource("/AdClickLog.csv")
        val inputStream = env.readTextFile(resource.getPath)

        //装换成样例类,提取时间戳和watermark
        val adLogStream = inputStream
            .map(data => {
                val arr = data.split(",")
                AdClickLog(arr(0).toLong,arr(1).toLong,arr(2),arr(3),arr(4).toLong)
            })
            .assignAscendingTimestamps(_.timeStamp * 1000L)  //升序数据,不需要考虑乱序

        // 插入一步过滤操作,并将有刷单行为的用户输出到侧输出流(黑名单报警)
        val filterBlackListUserStream:DataStream[AdClickLog] = adLogStream
            .keyBy(d => (d.userId,d.adId)) //同一个用户,同一个广告
            .process(new FilterBlackListUserResult(100)) //获取结果的操作


        //开窗聚合统计
        val adCountResultStream = filterBlackListUserStream
            .keyBy(_.province)
            .timeWindow(Time.days(1),Time.seconds(5))
//            .aggregate()
//            .process()  //或者

            //def aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation](
            //      preAggregator: AggregateFunction[T, ACC, V],
            //      windowFunction: WindowFunction[V, R, K, W]): DataStream[R]
            .aggregate(new AdCountAgg(),new AdCountWindowResult())


        adCountResultStream.print("countResult")
        filterBlackListUserStream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("warning")

        env.execute("ad count statistics job") //statistics 统计
    }
}

//public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable {
class AdCountAgg extends AggregateFunction[AdClickLog,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1
    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

//trait WindowFunction[IN, OUT, KEY, W <: Window] extends Function with Serializable {
class AdCountWindowResult extends WindowFunction[Long,AdClickCountByProvince,String, TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
        val end = new Timestamp(window.getEnd).toString
        out.collect(AdClickCountByProvince(end, key, input.head))
    }
}


//自定义KeyedProcessFunction
//public abstract class KeyedProcessFunction<K, I, O> extends AbstractRichFunction {
class FilterBlackListUserResult(maxCount:Long) extends KeyedProcessFunction[(Long,Long), AdClickLog,AdClickLog]{
    //用到了对应的状态,上面已经keyBy了,是针对同一个用户+同一个广告的次数,用valueState
    //定义了状态,保存用户对广告点击量,每天0点定时清空状态的时间戳
    lazy val countState:ValueState[Long] =  getRuntimeContext.getState(new ValueStateDescriptor[Long]("count",classOf[Long]))

    lazy val resetTimerState:ValueState[Long] =  getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts",classOf[Long]))

    //报警的数据侧输出流只需要输出一次就行了,标记当前用户是否已经进入黑名单
    lazy val isBlackState:ValueState[Boolean] =  getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isBlack",classOf[Boolean]))

    override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
        val curCount = countState.value()

        //判断只要是第一个数据来了,直接注册0点的清空状态的定时器
        if(curCount == 0L){
            val ts = ( ctx.timerService().currentProcessingTime()/(1000 * 24 * 60 * 60  ) + 1 ) * (24 * 60 * 60 * 1000)  - 8 * 60 * 60 * 1000  //明天0点,北京时间
            resetTimerState.update(ts)
            ctx.timerService().registerProcessingTimeTimer(ts)
        }

        //判断count值是否已经达到定义的阈值,如果达到就输出到侧输出流
        if(curCount >= maxCount){
            //判断是否已经在黑名单里了,没有的话才输出侧输出流
            if(! isBlackState.value()){
                isBlackState.update(true)
                //输出到侧输出流
                ctx.output(new OutputTag[BlackListUserWarning]("warning"), BlackListUserWarning(value.userId,value.adId,"Click ad over "+maxCount + " times today."))
            }
            //输出到侧输出流的就一次,
            return
        }

        //正常情况count+1,然后将数据原样输出
        countState.update(curCount + 1)
        out.collect(value)

    }

    //执行定时任务
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
        if(timestamp  == resetTimerState.value()){
            isBlackState.clear()
            countState.clear()
        }
    }
}
