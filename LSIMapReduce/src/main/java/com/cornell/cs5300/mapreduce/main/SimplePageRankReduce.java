package com.cornell.cs5300.mapreduce.main;

import java.io.IOException;

import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class SimplePageRankReduce extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		StringBuilder reducerOutput = new StringBuilder(key.toString());
		StringBuilder adjList = new StringBuilder();
		String outDegree = null;
		Double newPagerank = 0.0;
		Double oldPagerank = null;
		System.out.println("ITERATOR INSIDE REDUCER" + values);
		
		for(Text valueTxt:values) {

			String value = valueTxt.toString();

			if (value.contains(Constants.GRAPH_IDENTIFIER)) {
				String vals[] = value.split(" ");
				if (vals.length > 3) {
					for (int i = 3; i < vals.length; i++) {
						adjList.append(vals[i]).append(" ");
					}
					outDegree = vals[1];
				} else {
					outDegree = "0";
				}
				oldPagerank = Double.parseDouble(vals[2]);

			} else {
				newPagerank += Double.parseDouble(value);
			}

		}
		newPagerank = newPagerank * Constants.D_PR + (1 - Constants.D_PR) / Constants.N;
		reducerOutput.append(" ").append(outDegree.trim()).append(" ").append(newPagerank).append(" ")
				.append(adjList.toString().trim());
		Text reducerOutTxt = new Text();
		reducerOutTxt.set(reducerOutput.toString());
		Text keytmp = new Text("");

		context.write(keytmp, reducerOutTxt);

		double residual = Math.abs(oldPagerank - newPagerank) / newPagerank;
		long residualLong = (long) residual * 1000000;
		context.getCounter(Counter.COUNTER).increment(residualLong);

	}
}
