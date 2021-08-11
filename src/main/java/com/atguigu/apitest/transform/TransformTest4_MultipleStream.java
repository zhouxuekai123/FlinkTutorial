package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;

public class TransformTest4_MultipleStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件获取
        DataStream<String> inputStream = env.readTextFile("D:\\projects\\FlinkTutorial\\src\\main\\resources\\sensor.txt");
        //转换成sensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });
        //1 分流plit，按照温度值30度为界分为两条流
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        DataStream<SensorReading> allTempStream = splitStream.select("high","low");

        highTempStream.print("high");
        lowTempStream.print("low");
        allTempStream.print("all");

        //2 合流connect 将高温流转换成二元组类型，与低温流合并之后，输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = highTempStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectStream = warningStream.connect(lowTempStream);

        DataStream<Tuple3<String, Double, String>> resultStream = connectStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map1(Tuple2<String, Double> value1) throws Exception {
                return new Tuple3<>(value1.f0, value1.f1, "warning");
            }

            @Override
            public Tuple3<String, Double, String> map2(SensorReading value2) throws Exception {
                return new Tuple3<>(value2.getId(), value2.getTemperature(), "healthy");
            }
        });

        resultStream.print();


        //3 union合流
        DataStream<SensorReading> unionStream = highTempStream.union(lowTempStream,allTempStream);
        unionStream.print("union");



        env.execute();

    }
}
