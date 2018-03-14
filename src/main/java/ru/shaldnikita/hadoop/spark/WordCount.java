package ru.shaldnikita.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {


    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:/PROGRAMMING/winutils");

        SparkConf conf = new SparkConf();
        conf.setAppName("Word counter");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(/*"file:/home/shaldnikita/Downloads/titanic.csv"*/"inputDir");

        JavaPairRDD<String, Integer> counts = rdd
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile("output");

    }
}
