package com.cornell.cs5300.mapreduce.main;

/**
 * Hello world!
 *
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapOutputCollector.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimplePageRankingMain {

	
	public static void main(String[] args) throws Exception {

		
		System.out.println("ARG0 " + args[0]);
		System.out.println("ARG1 " + args[1]);
		String inputInitial = args[0];
		String output = args[1];

		int iteration = 1;
		String input = null;
		StringBuilder outStr = new StringBuilder();
		while (iteration <= 2) {
			
			Job conf = Job.getInstance(new Configuration(), "PageRank");

			if (iteration == 1) {
				input = inputInitial;

			} else {
				input = output + (iteration - 1);
			}
			output = output + iteration;


			conf.setJarByClass(SimplePageRankingMain.class);
			conf.setMapperClass(SimplePageRankMap.class);
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
			conf.setReducerClass(SimplePageRankReduce.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			conf.waitForCompletion(true);

			long longResidue = conf.getCounters().findCounter(Constants.Counter.COUNTER).getValue();
			double residual = longResidue / 1000000;
			residual /= Constants.N;

			System.out.println("--------------------Inside Iteration----------------");
			System.out.println("Iternation number " + iteration + " residual " + residual);

			if (residual <= Constants.ERROR_THRESHHOLD)
				break;

			// reset residual counter after each iteration
			conf.getCounters().findCounter(Constants.Counter.COUNTER).setValue(0);
			iteration++;
		}

		System.out.println("-----------------------------------------------------------------");
		System.out.println("---------------------End of PageRanking--------------------------");
		System.out.println("Iteration number " + iteration);

	}
}