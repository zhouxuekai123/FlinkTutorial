package com.atguigu.apitest.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //从文件获取
//        DataStream<String> inputStream = env.readTextFile("D:\\projects\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 从socket文本流中读取数据
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        //转换成String类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        })
                //升序数据，设置时间戳和watermark
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading sensorReading) {
//                        return sensorReading.getTimestamp()*1000L;
//                    }
//                })

                //乱序数据，设置时间戳和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(SensorReading sensorReading) {
                        return sensorReading.getTimestamp()*1000L;
                    }
                });

        DataStream<Integer> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer acc) {
                        return acc + 1;
                    }

                    @Override
                    public Integer getResult(Integer acc) {
                        return acc;
                    }

                    @Override
                    public Integer merge(Integer acc1, Integer acc2) {
                        return acc1+acc2;
                    }
                });
        //打印输出
        resultStream.print();

        //执行
        env.execute();

    }
}
