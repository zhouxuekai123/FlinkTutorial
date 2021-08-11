package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkTest4_Jdbc {
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

        dataStream.addSink(new MyJdbcSink());

        env.execute();

    }

    //实现自定义的sinkFunction
    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        Connection conn = null;
        PreparedStatement inserStmt = null;
        PreparedStatement updateStmt = null;

        //open主要创建连接
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
            //创建预编译语句，有占位符，可传入参数
            inserStmt = conn.prepareStatement("insert into sensor_tmp (id,temp) values (?,?)");
            updateStmt = conn.prepareStatement("update sensor_tmp set temp=? where id=?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            //执行更新语句，注意不留super
            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();
            //如果没有update语句，name就插入
            if (updateStmt.getUpdateCount() == 0) {
                inserStmt.setString(1, value.getId());
                inserStmt.setDouble(2, value.getTemperature());
                inserStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            updateStmt.close();
            inserStmt.close();
            conn.close();
        }
    }
}