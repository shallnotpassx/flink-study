package com.gongbotao.flink;

import java.util.Arrays;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Wave
 * @version v 0.1 2021/6/28 21:27
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        //1. create environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2. load file
        DataSet<String> inputSet = env
            .readTextFile("F:\\Project\\flink-study\\flink\\src\\main\\resources\\test.txt");

        //统计
        DataSet<Tuple2<String, Integer>> resultSet = inputSet
            .flatMap(new MyFlatMap())
            .groupBy(0)
            .sum(1);

        resultSet.print();
    }

    static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] wordArr = line.split(" ");
            Arrays.stream(wordArr).forEach(word -> {
                out.collect(new Tuple2<>(word, 1));
            });
        }
    }
}
