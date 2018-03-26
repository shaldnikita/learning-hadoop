package ru.shaldnikita.hadoop.inputFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalTime;

public class TimeUrlMapper extends Mapper<LocalTime,Text,LocalTime,Text>{

    @Override
    protected void map(LocalTime key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
