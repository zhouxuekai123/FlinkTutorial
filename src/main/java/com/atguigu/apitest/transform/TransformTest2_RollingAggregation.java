package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件获取
        DataStream<String> inputStream = env.readTextFile("D:\\projects\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //转换成sensorReading类型
//        DataStream<SensorReading> dataStream=inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//            String [] fields=s.split(",");
//                return new SensorReading(fields[0],Long.valueOf(fields[1]),Double.valueOf(fields[2]));
//            }
//        });

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        //分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        //KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(line -> line.getId());

        //滚动聚合，取当前最大的温度值
        //只温度变，其他字段不变
       // SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.max("temperature");

        //所有字段都变,显示最大温度值所在的行记录
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.maxBy("temperature");
        resultStream.print();

        env.execute();
    }
}
