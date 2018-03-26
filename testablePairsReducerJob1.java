/* 
 * Reducer class for CPE 369 Project for Dr. Stanchev
 * By Aleksander Braksator, William Eggert, Ryan Moore
 * 
 * This combiner is part of the testable_pairs Map/Reduce job which is extracting all 
 * pairs of batters and pitchers who individually appear at least N times in the 2010-2015
 * training data set, as well as at least M times together in the testing set.
*/

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;

public class testablePairsReducerJob1 extends Reducer<Text, IntWritable, Text, IntWritable> {
   
    public static final int n = 100;

    public void reduce(Text player, Iterable<IntWritable> count, Context context)
                throws IOException, InterruptedException {


        int sum=0;

        for(IntWritable ones : count){
            sum += ones.get();
        }

	if(sum>n){
            context.write(new Text(player), new IntWritable(sum));
        }

    }

}
