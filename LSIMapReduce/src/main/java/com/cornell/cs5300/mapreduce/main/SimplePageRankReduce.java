package com.cornell.cs5300.mapreduce.main;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapOutputCollector.Context;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;

import org.apache.hadoop.mapreduce.Reducer;

import com.sun.tools.javac.code.Attribute.Array;

public class SimplePageRankReduce extends Reducer<Text, Text, Text, Text> {
	Text test = new Text();

	public void reduce(Text key, Iterator<Text> values,Context context)
			throws IOException, InterruptedException {

		StringBuilder reducerOutput = new StringBuilder(key.toString());
		StringBuilder adjList = new StringBuilder();
		String outDegree = null;
		Double newPagerank = 0.0;
		Double oldPagerank = 0.0;
		while (values.hasNext()) {

			String value = values.next().toString();

			if (value.contains("graph_")) {
				String vals[] = value.split(" ");

				for (int i = 3; i < vals.length; i++) {
					adjList.append(vals[i]).append(" ");
				}
				outDegree = vals[1];
				oldPagerank = Double.parseDouble(vals[2]);

			}
			{
				newPagerank += Double.parseDouble(value);
			}

		}
		newPagerank= newPagerank*Constants.D_PR +  (1-Constants.D_PR)/Constants.N;
		reducerOutput.append(" ").append(outDegree.trim()).append(" ").append(newPagerank).append(" ")
				.append(adjList.toString().trim());
		Text reducerOutTxt = new Text();
		reducerOutTxt.set(reducerOutput.toString());
		context.write(key, reducerOutTxt);
		
		double residual = Math.abs(oldPagerank -  newPagerank) / newPagerank;
		long residualLong = (long) residual *100000;
		context.getCounter(Constants.Counter.COUNTER).increment(residualLong);
		
		
	}
}
