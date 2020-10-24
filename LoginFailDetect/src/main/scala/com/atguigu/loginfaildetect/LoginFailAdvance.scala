package com.atguigu.loginfaildetect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @description:
 * @author: malichun
 * @time: 2020/10/14/0014 17:10
 *
 *        改进,2秒登录失败
 *
 */
object LoginFailAdvance {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //读取数据,乱序数据
        val resource = getClass.getResource("/LoginLog.csv")
        val inputStream = env.readTextFile(resource.getPath)

        //转换成样例类类型,并提取时间戳和watermark
        val loginEventStream = inputStream
            .map(data => {
                val arr = data.split(",")
                LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(2)) {
                override def extractTimestamp(element: LoginEvent): Long = {
                    element.timestamp * 1000L
                }
            })
        val loginFailWarningStream = loginEventStream
                .keyBy(_.userId)
                .process( new LoginFailWarningAdvanceResult()) //上面已经keyBy了,这边是针对每个userId的统计

        loginFailWarningStream.print()

        env.execute("login fail detect job")
    }
}

//public abstract class KeyedProcessFunction<K, I, O> extends AbstractRichFunction
class LoginFailWarningAdvanceResult extends  KeyedProcessFunction[Long,LoginEvent,LoginFailWarning]{
    //用到一些状态
    // 保存当前所有的登录失败事件
    lazy val loginFailListState:ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list",classOf[LoginEvent]))


    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
        //首先判断事件类型
        if(value.eventType == "fail"){
            //1. 如果是失败,进一步做判断
            val iter = loginFailListState.get().iterator()
            //判断之前是否有登录失败事件
            if(iter.hasNext){
                //1.1 如果有,那么判断两次失败的时间差
                val firstFailEvent = iter.next()
                if(value.timestamp < firstFailEvent.timestamp + 2){
                    //如果在2s之内,输出报警
                    out.collect(LoginFailWarning(value.userId,firstFailEvent.timestamp,value.timestamp,"login fail 2 times in 2 seconds"))
                }

                //不管报不报警,当前都已处理完毕.将状态更新为最近1次登录失败的事件
                loginFailListState.clear()
                loginFailListState.add(value)

            }else{
                //1.2 如果没有,直接把当前事件添加到listState中
                loginFailListState.add(value)
            }

        }else{
            //2. 登录成功,直接清空状态
            loginFailListState.clear()
        }

    }
}