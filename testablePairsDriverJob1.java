/* 
 * Driver class for CPE 369 Project for Dr. Stanchev
 * By Aleksander Braksator, William Eggert, Ryan Moore
 * 
 * This driver is part of the testable_pairs Map/Reduce job which is extracting all 
 * pairs of batters and pitchers who individually appear at least N times in the 2010-2015
 * training data set, as well as at least M times together in the testing set.
*/


import org.apache.log4j.Logger;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class testablePairsDriverJob1 extends Configured implements Tool {


private static final Logger THE_LOGGER = Logger.getLogger(testablePairsDriverJob1.class);



	@Override
	public int run(String[] args) throws Exception {


		Job job = Job.getInstance();	


		job.setJarByClass(testablePairsDriverJob1.class);


		job.setJobName("testablePairsJob1");


		job.setOutputKeyClass(Text.class);


		job.setOutputValueClass(IntWritable.class);


		job.setMapOutputKeyClass(Text.class);


		job.setMapOutputValueClass(IntWritable.class);


		job.setMapperClass(testablePairsMapperJob1.class);

		job.setCombinerClass(testablePairsCombinerJob1.class);
		job.setReducerClass(testablePairsReducerJob1.class);

                job.getConfiguration().setInt("N", 100);

		FileInputFormat.setInputPaths(job, new Path(args[0]));


		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		boolean status = job.waitForCompletion(true);


		THE_LOGGER.info("run(): status=" + status);


		return status ? 0 : 1;


	}


	public static void main(String[] args) throws Exception {


		if (args.length != 2) {


			throw new IllegalArgumentException("usage: <input> <output>");

	}




	THE_LOGGER.info("inputDir = " + args[0]);


	THE_LOGGER.info("outputDir = " + args[1]);


	int returnStatus = ToolRunner.


	run(new testablePairsDriverJob1(), args);


	THE_LOGGER.info("returnStatus=" + returnStatus);


	System.exit(returnStatus);


	}


}
