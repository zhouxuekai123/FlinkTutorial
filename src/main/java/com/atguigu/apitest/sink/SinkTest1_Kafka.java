package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件获取
        DataStream<String> inputStream = env.readTextFile("D:\\projects\\FlinkTutorial\\src\\main\\resources\\sensor.txt");
        //转换成String类型
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2])).toString();
        });


        dataStream.addSink(new FlinkKafkaProducer011<String>("localhost:9092","topic_name",new SimpleStringSchema()));

        env.execute();

    }
}
