package com.cornell.cs5300.mapreduce.main;

/**
 * Hello world!
 *
 */



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapOutputCollector.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimplePageRankingMain {

	/*
	 * public static class Map extends MapReduceBase implements
	 * Mapper<LongWritable, Text, Text, Text> { private final static IntWritable
	 * one = new IntWritable(1); private Text word = new Text();
	 * 
	 * public void map(LongWritable key, Text value, OutputCollector<Text, Text>
	 * output, Reporter reporter) throws IOException { String line =
	 * value.toString(); // StringTokenizer tokenizer = new
	 * StringTokenizer(line); Text str = new Text(); str.set(line); String out[]
	 * = line.split(" "); Text keyNode = new Text(); keyNode.set(out[0]);
	 * output.collect(keyNode, str); int outDegree = out.length - 2; double
	 * pgrval = Double.parseDouble((out[out.length - 1])) / outDegree; for (int
	 * i = 1; i < out.length - 1; i++) { System.out.println(
	 * "inside for loop with out length = " + out.length); Text pgrank = new
	 * Text(); pgrank.set(String.valueOf(pgrval)); Text reducesKey = new Text();
	 * reducesKey.set(out[0]); output.collect(reducesKey, pgrank);
	 * 
	 * }
	 * 
	 * } }
	 * 
	 * public static class Reduce extends MapReduceBase implements Reducer<Text,
	 * Text, Text, Text> { Text test = new Text();
	 * 
	 * public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,
	 * Text> output, Reporter reporter) throws IOException {
	 * 
	 * test.set("hello"); StringBuilder str = new StringBuilder(); while
	 * (values.hasNext()) {
	 * 
	 * str.append(values.next().toString()); }
	 * 
	 * Text reducerVal = new Text(); reducerVal.set(str.toString());
	 * 
	 * output.collect(key, reducerVal); } }
	 */

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
	
			//conf.setCombinerClass(SimplePageRankReduce.class);
			conf.setReducerClass(SimplePageRankReduce.class);

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

			long longResidue = conf.getCounters().findCounter(Counter.COUNTER).getValue();
			double residual = longResidue / 1000000;
			residual /= Constants.N;

			System.out.println("--------------------Inside Iteration----------------");
			System.out.println("Iternation number " + iteration + " residual " + residual);

			if (residual <= Constants.ERROR_THRESHHOLD)
				break;

			// reset residual counter after each iteration
			conf.getCounters().findCounter(Counter.COUNTER).setValue(0);
			iteration++;
		}

		System.out.println("-----------------------------------------------------------------");
		System.out.println("---------------------End of PageRanking--------------------------");
		System.out.println("Iteration number " + iteration);

	}
}