package com.ibaqu.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.io.IOException;
import java.net.http.WebSocket;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class MovieRecommendations {

    public static class Map extends Mapper<LongWritable, Text, Text, ArrayWritable> {

        private final int USER_ID = 0;
        private final int MOVIE_ID = 1;
        private final int MOVIE_RATING = 2;

        // --- Input ---
        // Format   : UserID::MovieID::Rating::Timestamp
        // Sample   :
        //      17 70 3
        //      35 21 1
        //      49 19 2
        //      49 21 1
        //      49 70 4
        //      87 19 1
        //      87 21 2
        //      98 19 2

        public void map(LongWritable key, Text fileContents, Context context) throws IOException, InterruptedException {
            String line = fileContents.toString();
            String[] values = line.split("::");

            String userId = values[USER_ID];
            String movieId = values[MOVIE_ID];
            String movieRating = values[MOVIE_RATING];

            Text userId_Text = new Text(userId);
            Text movieId_Text = new Text(movieId);
            Text movieRating_Text = new Text(movieRating);

            ArrayWritable movieIdAndRating = new ArrayWritable(Text.class);
            movieIdAndRating.set(new Text[] { movieId_Text, movieRating_Text });

            context.write(userId_Text, movieIdAndRating);
        }

        // Output : UserID, (MovieID , Rating) After shuffling im assuming
        //      17     ( [70,3] )
        //      35     ( [21,1] )
        //      49     ( [19,2 | 21,1 | 70,4] )
        //      87     ( [19,1 | 21,2] )
        //      98     ( [19,2])
    }

    public static class Reduce extends Reducer<Text, ArrayWritable, Text, CustomWritable> {

        public void reduce(Text userId, Iterable<ArrayWritable> movieIdAndRatingArray, Context context)
                throws IOException, InterruptedException{

            int movieCount = 0;    // Number of movies watched per person
            int ratingSum = 0;     // Sum of the movie rating

            // Go through each Movie and Rating in values
            // Sample : ( [19,2 | 21,1 | 70,4] )
            for (ArrayWritable movieIdAndRating : movieIdAndRatingArray) {
                movieCount += 1;

                // Extract movieId and movieRating
                // [movieId , movieRating]   [19, 2]
                Text movieId = (Text) movieIdAndRating.get()[0];
                Text movieRating = (Text) movieIdAndRating.get()[1];

                // Aggregate all ratings
                ratingSum += Integer.parseInt(movieRating.toString());
            }

            // Value : (movie_count , rating_sum, [ (MovieID , Rating), (MovieID, Rating)])

            IntWritable movieCountInt = new IntWritable(movieCount);
            IntWritable ratingSumInt = new IntWritable(ratingSum);

            // Initialize our Custom Writable with IntWritable, IntWritable, Iterable<ArrayWritable>
            CustomWritable customWritable = new CustomWritable(movieCountInt, ratingSumInt, movieIdAndRatingArray);

            context.write(userId, customWritable);
        }

    }

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

}
