package ru.shaldnikita.hadoop.inputFormat;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;

import java.io.IOException;
import java.net.URL;
import java.time.LocalTime;

public class TimeUrlLineRecordReader extends RecordReader<LocalTime, URLWritable> {

    private KeyValueLineRecordReader lineRecordReader;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        lineRecordReader = new KeyValueLineRecordReader(context.getConfiguration());

        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        return lineRecordReader.nextKeyValue();
    }

    @Override
    public LocalTime getCurrentKey() {
        return LocalTime.parse(lineRecordReader.getCurrentKey().toString());
    }

    @Override
    public URLWritable getCurrentValue() throws IOException {
        return new URLWritable(lineRecordReader.getCurrentValue().toString());
    }

    @Override
    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}
