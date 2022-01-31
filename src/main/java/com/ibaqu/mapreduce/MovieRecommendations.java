package com.ibaqu.mapreduce;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MovieRecommendations {

    public static class TextArrayWritable extends ArrayWritable {

        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }

        @Override
        public Writable[] get() {
            return super.get();
        }
    }

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

            if (!line.isEmpty()) {
                String[] values = line.split("-");

                String userId = values[USER_ID];
                String movieId = values[MOVIE_ID];
                String movieRating = values[MOVIE_RATING];

                Text userId_Text = new Text(userId);
                TextArrayWritable test = new TextArrayWritable(new String[] { movieId, movieRating });

                context.write(userId_Text, test);
            }
        }

        // Output : UserID, (MovieID , Rating) After shuffling im assuming
        //      17     ( [70,3] )
        //      35     ( [21,1] )
        //      49     ( [19,2 | 21,1 | 70,4] )
        //      87     ( [19,1 | 21,2] )
        //      98     ( [19,2])
    }

    public static class Reduce extends Reducer<Text, TextArrayWritable, Text, Text> {

        public static String COMMA = ",";
        public static String SEPARATOR = "|";
        public static String HYPHEN = "-";

        public void reduce(Text userId, Iterable<TextArrayWritable> movieIdAndRatingArray, Context context)
                throws IOException, InterruptedException{

            int movieCount = 0;    // Number of movies watched per person
            int ratingSum = 0;     // Sum of the movie rating
            String value = "";

            // Go through each Movie and Rating in values
            // Sample : ([19,2 | 21,1 | 70,4] )
            for (TextArrayWritable movieIdAndRating : movieIdAndRatingArray) {
                // Tally the number of movies
                movieCount += 1;

                // Extract movieId and movieRating
                // [movieId , movieRating]   [19, 2]
                Text movieId = (Text) movieIdAndRating.get()[0];
                Text movieRating = (Text) movieIdAndRating.get()[1];

                // Append the movie|rating pair to the 'value' string
                value += movieId.toString() + SEPARATOR + movieRating.toString();
                value += HYPHEN;

                // Aggregate all ratings for the movie
                ratingSum += Integer.parseInt(movieRating.toString());
            }

            // Remove last hyphen
            value = StringUtils.substring(value, 0, value.length()-1);

            // Append movieCount and ratingSum
            value += COMMA + String.valueOf(movieCount);
            value += COMMA + String.valueOf(ratingSum);

            // Final value should be written like this :
            // userId       movieId|movieRating-movieId|movieRating,movieCount,ratingSum
            context.write(userId, new Text(value));
        }

        // Output : UserId  movieId|rating...,movieCount,ratingSum
        //  17	    70|3,1,3
        //  35	    21|1,1,1
        //  49	    70|4-21|1-19|2,3,7
        //  87	    21|2-19|1,2,3
        //  98	    19|2,1,2
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(MovieRecommendations.class);

        // Specify job
        job.setJobName("movie_recommendations");

        // Set Output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

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
