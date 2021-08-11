package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        String inputPath="D:\\projects\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDateSet = env.readTextFile(inputPath);

        //空格打散之后 对单词进行group分组 然后sum进行聚合
        DataSet<Tuple2<String,Integer>> wordCountDataSet= inputDateSet.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);


        //打印输出
        wordCountDataSet.print();
    }
   public static class  MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{
       @Override
       public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
           String[] words = value.split(" ");
           for (String word:words){
               collector.collect(new Tuple2<>(word,1));
           }
       }
   }

}
