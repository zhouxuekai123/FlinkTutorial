package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class SinkTest3_Es {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件获取
        DataStream<String> inputStream = env.readTextFile("D:\\projects\\FlinkTutorial\\src\\main\\resources\\sensor.txt");
        //转换成String类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        //自定义es的链接配置
        ArrayList<HttpHost> httpHosts=new ArrayList<>();
        httpHosts.add(new HttpHost("localhost",9200));

        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts,new MyEsSinkMapper()).build());

        env.execute();

    }

    public static class MyEsSinkMapper implements ElasticsearchSinkFunction<SensorReading>{
        @Override
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id",sensorReading.getId());
            dataSource.put("temp",sensorReading.getTemperature().toString());
            dataSource.put("ts",sensorReading.getTimestamp().toString());

            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("readingData")
                    .source(dataSource);

            requestIndexer.add(indexRequest);
        }
    }

}
