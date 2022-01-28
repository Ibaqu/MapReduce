package com.ibaqu.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MovieRecommendations {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(MovieRecommendations.class);

        // Specify job
        job.setJobName("movie_recommendations");

        // Set Output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set Map and Reduce classes
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {


    }
}
