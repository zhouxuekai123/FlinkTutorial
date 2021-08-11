package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置线程数
        env.setParallelism(2);

//        //从文件中读取数据
//        String inputPath="D:\\projects\\FlinkTutorial\\src\\main\\resources\\hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        //用 paramter tool 工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host=parameterTool.get("host");
        Integer port =parameterTool.getInt("port");

        // 从socket文本流中读取数据
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        //基于数据流计算
        DataStream<Tuple2<String,Integer>> wordCountDataStream= inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);

        //打印输出
        wordCountDataStream.print();

        //执行任务 提前启动cmd窗口  nc -l -p 7777
        env.execute();
    }


}
