package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkTest2_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件获取
        DataStream<String> inputStream = env.readTextFile("D:\\projects\\FlinkTutorial\\src\\main\\resources\\sensor.txt");
        //转换成String类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        //定义jedis链接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        dataStream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        env.execute();

    }

        //自定义RedisMapper
    public static class MyRedisMapper implements RedisMapper<SensorReading> {
        //定义保存数据到redis的命令，存成hash表，hset   表名称sensor_tmp
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor");
        }

        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.getId();
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return String.valueOf(sensorReading.getTemperature());
        }
    }
}
