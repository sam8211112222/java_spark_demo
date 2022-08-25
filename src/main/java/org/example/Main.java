package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        List<Double> inputData = new ArrayList<Double>();
        inputData.add(34.6);
        inputData.add(12.4324);
        inputData.add(3242.32);
        inputData.add(20.10);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> myRdd = sc.parallelize(inputData);
        Double result = myRdd.reduce((value1,value2) -> value1+value2);
        System.out.println(result);
        sc.close();
    }
}
