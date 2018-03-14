package ru.shaldnikita.hadoop.wordcount.old;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer st = new StringTokenizer(line," \t\n\r\f,.(){}:;?![]'");

        while (st.hasMoreTokens()) {
            word.set(st.nextToken().toLowerCase());
            context.write(word, one);
        }

    }
}