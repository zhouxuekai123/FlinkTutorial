package com.atguigu.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File{
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件获取
        DataStream<String> dataStream = env.readTextFile("D:\\projects\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //打印输出
        dataStream.print();

        //执行
        env.execute();

    }
}
