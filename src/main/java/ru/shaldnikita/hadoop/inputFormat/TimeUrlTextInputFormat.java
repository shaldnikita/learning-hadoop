package ru.shaldnikita.hadoop.inputFormat;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.time.LocalTime;

public class TimeUrlTextInputFormat extends FileInputFormat<LocalTime,URLWritable> {
    @Override
    public RecordReader<LocalTime, URLWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new TimeUrlLineRecordReader();
    }
}
