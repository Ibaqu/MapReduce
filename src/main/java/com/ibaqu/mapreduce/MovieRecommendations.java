package com.ibaqu.mapreduce;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MovieRecommendations {
    // Custom Writable to pass Text Arrays between Mapper and Reducer
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

    public static class Map1 extends Mapper<LongWritable, Text, Text, ArrayWritable> {

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
                String[] values = line.split("::");

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

    public static class Reduce1 extends Reducer<Text, TextArrayWritable, Text, Text> {

        private static String COMMA = ",";
        private static String SEPARATOR = "/";
        private static String HYPHEN = "-";
        private static int MOVIEID = 0;
        private static int RATING = 1;

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
                Text movieId = (Text) movieIdAndRating.get()[MOVIEID];
                Text movieRating = (Text) movieIdAndRating.get()[RATING];

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

    public static class Map2 extends Mapper<LongWritable, Text, Text, ArrayWritable> {

        public static String COMMA = ",";
        public static String HYPHEN = "-";
        public static String SEPARATOR = "/";

        public static int MOVIE = 0;
        public static int RATING = 1;

        // Output : UserId  movieId|rating...,movieCount,ratingSum
        //  17	    70|3,1,3
        //  35	    21|1,1,1
        //  49	    70|4-21|1-19|2,3,7
        //  87	    21|2-19|1,2,3
        //  98	    19|2,1,2

        public void map(LongWritable key, Text fileContents, Context context) throws IOException, InterruptedException {
            String line = fileContents.toString();

            String values = line.split("\t")[1];    //  70|4-21|1-19|2,3,7

            String[] movieDetails = values.split(COMMA);    // 70|4-21|1-19|2   3   7

            String[] movieIdsAndRatings = movieDetails[0].split(HYPHEN);
            String movieCount = movieDetails[1];

            // For 3 movies, we can assign each an index 0, 1, 2
            // Find the combinations like [0, 1] , [0, 2], [1, 2].
            // These will correspond to the movie and its rating
            // Take each pair and add to context

            if (Integer.parseInt(movieCount) > 1) {
                // 70|4 21|1 19|2 - movieIdsAndRatings

                // Get combination
                List<int[]> indexPairs = Utils.generate(Integer.parseInt(movieCount), 2);

                // For each indexPair, we get the pairs from movieIdsAndRatings
                for (int[] indexPair : indexPairs) {

                    // ex. indexPair = [1, 2]
                    String movieIdAndRating_0 = movieIdsAndRatings[indexPair[0]]; // movieIdsAndRatings[1] --> 21|1
                    String movieIdAndRating_1 = movieIdsAndRatings[indexPair[1]]; // movieIdsAndRatings[2] --> 19|2

                    // Just splits it into two
                    String[] movieIdAndRating0_array = movieIdAndRating_0.split(SEPARATOR); // 21   1
                    String[] movieIdAndRating1_array = movieIdAndRating_1.split(SEPARATOR); // 19   2

                    String[] movieIdsPair = { movieIdAndRating0_array[MOVIE], movieIdAndRating1_array[MOVIE]};  // 21,19
                    String[] ratingsPair = { movieIdAndRating0_array[RATING], movieIdAndRating1_array[RATING]}; // 1,2

                    Text movieIdsPairText = new Text(Arrays.toString(movieIdsPair));
                    TextArrayWritable ratingsPairWritable = new TextArrayWritable(ratingsPair);

                    context.write(movieIdsPairText, ratingsPairWritable);
                }
            }
        }
    }

    public static class Reduce2 extends Reducer<Text, TextArrayWritable, Text, Text> {

        private static int RATING_X = 0;
        private static int RATING_Y = 1;

        public void reduce(Text movieIdPair, Iterable<TextArrayWritable> ratingPair,
                           Context context) throws IOException, InterruptedException {
            // Based on all users ratings, all we get is the movie pair. Using these movie pairs, we take the ratings
            // pair and create 2 arrays which we will feed into the Pearson Correlation values.
            // ex. 21,19    1,2 | 2, 1 | 3, 5 | .... (for the multiple users that rated these)
            // Output should be the correlation between the pair only

            // Go through each Rating pair and take each individual rating and put them in a double array
            List<Double> rxDoubleList = new ArrayList<>();
            List<Double> ryDoubleList = new ArrayList<>();

            for (TextArrayWritable rating : ratingPair) {
                // Extract Text value of each movie rating
                Text rating_x = (Text) rating.get()[RATING_X];
                Text rating_y = (Text) rating.get()[RATING_Y];

                // Convert to double
                Double rxDouble = Double.valueOf(rating_x.toString());
                Double ryDouble = Double.valueOf(rating_y.toString());

                // Add to list
                rxDoubleList.add(rxDouble);
                ryDoubleList.add(ryDouble);
            }

            double[] rxdoubleArray = ArrayUtils.toPrimitive(rxDoubleList.toArray(new Double[0]));
            double[] rydoubleArray = ArrayUtils.toPrimitive(ryDoubleList.toArray(new Double[0]));

            // Calculate the correlation
            try {
                double correlation = new PearsonsCorrelation().correlation(rxdoubleArray, rydoubleArray);

                if (!Double.isNaN(correlation)) {
                    Text corr = new Text(String.valueOf(correlation));
                    context.write(movieIdPair, corr);
                }
            } catch (MathIllegalArgumentException e) {
                // Ignore illegal arguments that may arise in the data
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job1 = Job.getInstance();
        job1.setJarByClass(MovieRecommendations.class);

        // Specify job
        job1.setJobName("movie_recommendations1");

        // Set Overall Output
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // Set [MAP] output key and value
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(TextArrayWritable.class);

        // Set input and output format class
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        // Set class arguments
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // Set Map and Reduce classes
        job1.setMapperClass(Map1.class);
        job1.setReducerClass(Reduce1.class);

        // Submit the job, then poll for progress until the job is complete
        job1.waitForCompletion(true);

        // ------------------------------------------------------------------- //
        Job job2 = Job.getInstance();
        job2.setJarByClass(MovieRecommendations.class);

        // Specify job
        job2.setJobName("movie_recommendations2");

        // Set Output
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Set [MAP] output key and value
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(TextArrayWritable.class);

        // Set input and output format class
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // Set class arguments
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // Set Map and Reduce classes
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

}
