package com.atguigu.networkflow_analysis

import com.atguigu.networkflow_analysis.UniqueVisitor.getClass
import com.atguigu.networkflow_analysis.uv.{Bloom, MyTrigger, UvCountWithBloom}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * Created by John.Ma on 2020/10/4 0004 20:46
 */
object UvWithBloom {
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
            .map(data => ("uv", data.userId))
            .keyBy(_._1)
            .timeWindow(Time.hours(1))
            .trigger(new MyTrigger()) //自定义触发器,直接触发一次后面的窗口计算
            .process(new UvCountWithBloom())

        uvStream.print()

        env.execute("uv with bloom job")
    }
}


//触发器,每来一条数据,直接触发窗口计算,并清空窗口状态
//public abstract class Trigger<T, W extends Window> implements Serializable {
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
    //每来一个数据的时候,清空状态,触发窗口计算
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.FIRE_AND_PURGE //purge:清除,清空状态,触发窗口计算
    }

    //系统时间有进展
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.CONTINUE
    }

    //有watermark改变的时候
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.CONTINUE //继续
    }

    //收尾清理工作
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {

    }
}

//自定义一个布隆过滤器
//主要要素是一个位图和hash函数
//放到redis
class Bloom(size: Long) extends Serializable {
    private val cap = size //默认cap应该是2的整次幂

    //hash函数
    def hash(value: String, seed: Int): Long = {
        var result = 0
        for( i <- 0 until value.length ){
            result = result * seed  + value.charAt(i)
        }

        //返回hash值,要映射到cap范围内
        (cap - 1) & result  //截取
    }
}


//实现自定义的窗口处理函数
//abstract class ProcessWindowFunction[IN, OUT, KEY, W <: Window]
class UvCountWithBloom() extends ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow]{

    //定义redis连接以及布隆过滤器
    lazy val jedis = new Jedis("172.16.189.210",6379)

    lazy val bloomFilter = new Bloom(1 << 29) //位的个数:2^6(64) * 2^20(1M) * 2*3(8bit),64M

    //本来是收集齐所有数据,窗口触发计算的时候才会调用,现在是每来一条都调用一次(trigger)
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
        jedis.select(14)

        // 先定义redis中存储的位图的key
        val storedBitMapKey = context.window.getEnd.toString

        // 另外将当前窗口的uv count值作为状态,保存到redis里,用一个叫做uvCount的Hash表来保存
        val uvCountMap = "uvcount"
        val currentKey = context.window.getEnd.toString
        var count = 0L
        //从redis中取出当前窗口的uv count值
        val jedisCount = jedis.hget(uvCountMap,currentKey)
        if(jedisCount != null){
            count = jedisCount.toLong
        }

        // 去重: 判断当前userId的hash值对应的位图位置,是否唯一,是为0
        val userId  = elements.last._2.toString
        //计算hash值,就对应着位图中的偏移量
        val offset = bloomFilter.hash(userId,61)
        //用redis的位操作命令,取bitMap中对应的值
        val isExist = jedis.getbit(storedBitMapKey,offset)
        if(!isExist){
            //如果不存在,那么位图对应的位置置为1,并且将count + 1
            jedis.setbit(storedBitMapKey,offset,true)
            jedis.hset(uvCountMap,currentKey,(count + 1).toString)
        }
    }

}