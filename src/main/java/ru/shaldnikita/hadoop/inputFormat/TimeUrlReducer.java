package ru.shaldnikita.hadoop.inputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TimeUrlReducer extends Reducer<LongWritable,Text,LongWritable,Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        Iterator<Text> valuesIt = values.iterator();

        while(valuesIt.hasNext()){
            sum +=1;
        }

        if(sum>3)
            context.write(key, new Text("!@3"));
    }
}
