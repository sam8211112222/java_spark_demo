package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        Double plusResult = plus(sc);
        Double sqrtResult = division(sc);
//        System.out.println(plusResult);
        sc.close();
    }

    public static Double plus(JavaSparkContext sc){
        List<Double> inputData = new ArrayList<Double>();
        inputData.add(34.6);
        inputData.add(12.4324);
        inputData.add(3242.32);
        inputData.add(20.10);
        JavaRDD<Double> myRdd = sc.parallelize(inputData);
        Double result = myRdd.reduce((value1,value2) -> value1+value2);
        return result;
    }

    public static Double division(JavaSparkContext sc){
        List<Integer> inputData = new ArrayList<Integer>();
        inputData.add(34);
        inputData.add(12);
        inputData.add(3242);
        inputData.add(49);
        JavaRDD<Integer> myRdd = sc.parallelize(inputData);
        JavaRDD <Double>sqrtRdd = myRdd.map((value) -> Math.sqrt(value));
        sqrtRdd.foreach(value -> System.out.println(value));
        //也可以這樣寫
        sqrtRdd.collect().forEach(System.out::println);

        // 單純用map, reduce可以知道資料筆數
        JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
        Long count = singleIntegerRdd.reduce((v1,v2) -> v1 + v2);
        System.out.println(count);
        return null;
    }
}
