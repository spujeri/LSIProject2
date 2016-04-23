package com.cornell.cs5300.mapreduce.main;

/**
 * Hello world!
 *
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PageRanking {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			// StringTokenizer tokenizer = new StringTokenizer(line);
			Text str = new Text();
			str.set(line);
			String out[] = line.split(" ");
			Text keyNode = new Text();
			keyNode.set(out[0]);
			output.collect(keyNode, str);
			int outDegree = out.length - 2;
			double pgrval = Double.parseDouble((out[out.length - 1])) / outDegree;
			for (int i = 1; i < out.length - 1; i++) {

				Text pgrank = new Text();
				pgrank.set(String.valueOf(pgrank));
				Text reducesKey = new Text();
				reducesKey.set(out[0]);
				output.collect(reducesKey, pgrank);

			}

		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		Text test = new Text();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			test.set("hello");
			StringBuilder str = new StringBuilder();
			while (values.hasNext()) {

				str.append(values.next().toString());
			}

			Text reducerVal = new Text();
			reducerVal.set(str.toString());

			output.collect(key, reducerVal);
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(PageRanking.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}