package com.atguigu.orderpay_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @description:
 * @author: malichun
 * @time: 2020/10/15/0015 17:35
 *
 *        34729,create,,1558430842
 *        34730,create,,1558430843
 *        34729,pay,sd76f87d6,1558430844
 *        34730,pay,3hu3k2432,1558430845
 *        34731,create,,1558430846
 *        34731,pay,35jue34we,1558430849
 *
 * 需求:
 *
 */
//定义输入输出样例类
case class OrderEvent(orderId:Long, eventType:String, txId:String,timestamp:Long)
case class OrderResult(orderId:Long, resultMsg:String)


object OrderTimeout {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //0.从文件中读取数据
        val resource = getClass.getResource("/OrderLog.csv")
        val orderEventStream = env.readTextFile(resource.getPath)
            .map( data => {
                val arr = data.split(",")
                OrderEvent(arr(0).toLong, arr(1),arr(2), arr(3).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)
            .keyBy(_.orderId) //只针对当前id

        //1.定义一个pattern
        val orderPayPattern = Pattern
            .begin[OrderEvent]("create").where(_.eventType == "create")
            .followedBy("pay").where(_.eventType == "pay")
            .within(Time.minutes(15))  //15分钟之内


        //2.将pattern应用到数据流上进行模式检测
        val patternStream = CEP.pattern(orderEventStream,orderPayPattern)

        //3.定义一个侧输出流标签,用于处理超时事件
        val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

        //4.调用select方法,提取并处理匹配的成功支付时间以及超时时间
        //def select[L: TypeInformation, R: TypeInformation](
        //    outputTag: OutputTag[L],
        //    patternTimeoutFunction: PatternTimeoutFunction[T, L],  //用于处理超时事件
        //    patternSelectFunction: PatternSelectFunction[T, R])   //用于处理匹配成功的输出
        //  : DataStream[R] =
        val resultStream = patternStream.select(
            orderTimeoutOutputTag,
            new OrderTimeoutSelect(),
            new OrderPySelect()
        )

        resultStream.print("payed")
        resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

        env.execute("order timeout job")
    }
}
//实现自定义patterTimeoutFunction以及PatternSelectFunction

//public interface PatternTimeoutFunction<IN, OUT>
class OrderTimeoutSelect extends  PatternTimeoutFunction[OrderEvent,OrderResult]{
    override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
        val timeoutOrderId = pattern.get("create").iterator().next().orderId
        OrderResult(timeoutOrderId," timeout : "+ timeoutTimestamp)


    }
}

// 正常数据的处理
//public interface PatternSelectFunction<IN, OUT> extends Function, Serializable
class OrderPySelect extends PatternSelectFunction[OrderEvent,OrderResult]{
    override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
        val payedOrderId = map.get("pay").iterator().next().orderId
        OrderResult(payedOrderId,"payed successfully")
    }
}


