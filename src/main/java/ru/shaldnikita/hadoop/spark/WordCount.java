package ru.shaldnikita.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Word counter");
        conf.setMaster("local");
        conf.


        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(/*"file:/home/shaldnikita/Downloads/titanic.csv"*/"input.txt");
        int wordCount = rdd
                .map(String::length)
                .reduce((a, b) -> a + b);
        System.out.println(wordCount);

    }
}
