package com.cornell.cs5300.mapreduce.main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimplePageRankMap extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		
		Text str = new Text();
		str.set(Constants.GRAPH_IDENTIFIER+line);
		String out[] = line.split(" ");
		Text keyNode = new Text();
		keyNode.set(out[0]);
		context.write(keyNode, str);
		Double pagerankVal = Double.parseDouble((out[2]));

		int outDegree = Integer.parseInt(out[1]);
		if (outDegree > 0 && out.length > 3) {
			double pgrval = pagerankVal / outDegree;
			for (int i = 3; i < out.length; i++) {
				Text pgrank = new Text();
				pgrank.set(String.valueOf(pgrval));
				Text reducesKey = new Text();
				reducesKey.set(out[0]);
				context.write(reducesKey, pgrank);

			}
		}

	}

}
