package com.ibaqu.mapreduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class CustomWritable implements Writable {

    private IntWritable movieCount;
    private IntWritable ratingSum;
    private Iterable<ArrayWritable> movieIdAndRatingArray;

//    public CustomWritable() {
//        movieCount = new IntWritable(0);
//        ratingSum = new IntWritable(0);
//        movieIdAndRatingArray = new Iterable<ArrayWritable>() {
//            @Override
//            public Iterator<ArrayWritable> iterator() {
//                return null;
//            }
//        };
//    }

    public CustomWritable(IntWritable movieCount, IntWritable ratingSum,
                          Iterable<ArrayWritable> movieIdAndRatingArray) {
        this.movieCount = movieCount;
        this.ratingSum = ratingSum;
        this.movieIdAndRatingArray = movieIdAndRatingArray;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        movieCount.write(dataOutput);
        ratingSum.write(dataOutput);

        for (ArrayWritable movieIdAndRating : movieIdAndRatingArray) {
            movieIdAndRating.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movieCount.readFields(dataInput);
        ratingSum.readFields(dataInput);

        for (ArrayWritable movieIdAndRating : movieIdAndRatingArray) {
            movieIdAndRating.readFields(dataInput);
        }
    }
}
