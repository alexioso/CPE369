/* 
 * Mapper class for CPE 369 Project for Dr. Stanchev
 * By Aleksander Braksator, William Eggert, Ryan Moore
 * 
 * This mapper is part of the testable_pairs Map/Reduce job which is extracting all 
 * pairs of batters and pitchers who individually appear at least N times in the 2010-2015
 * training data set, as well as at least M times together in the testing set.
*/

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;

public class testablePairsMapperJob1 extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

        String valueAsString = value.toString().trim();

        String[] tokens = valueAsString.split(",");

        if (tokens.length != 11) {
            return;
        }

        

        //assign a 1 to a key (name of baseball player) everytime they appear in the training data
        context.write(new Text("B,"+tokens[4]),new IntWritable(1));
        context.write(new Text("P,"+tokens[5]),new IntWritable(1));
    }
}

