package com.atguigu.apitest.source;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //自定义数据源
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        //打印输出
        dataStream.print();

        //执行
        env.execute();

    }

    //实现自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading> {
        //定义一个标志位，控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            //定义一个随机数生成器
            Random random = new Random();
            //设置10个传感器初始温度值
            HashMap<String, Double> sensorTmpMap = new HashMap<>();
            for (int i=0 ;i<10;i++){
                sensorTmpMap.put("sensor_"+(i+1),60+random.nextGaussian()*20);
            }

            while (running){
                for (String sensorId: sensorTmpMap.keySet()){
                    //在当前温度基础上随机波动
                    double newtemp = sensorTmpMap.get(sensorId) + random.nextGaussian();
                    sensorTmpMap.put(sensorId,newtemp);
                    ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),newtemp));
                }
               //控制输出频率
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
