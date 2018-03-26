package ru.shaldnikita.hadoop.inputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class Main {

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(Main.class);
        job.setJobName("TimeUrl");

        FileInputFormat.addInputPath(job, new Path("input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        job.setInputFormatClass(TimeUrlTextInputFormat.class);

        job.setMapperClass(TimeUrlMapper.class);
        job.setReducerClass(TimeUrlReducer.class);
        job.setCombinerClass(LongSumReducer.class);

        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        if (job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if (!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

    }
}
