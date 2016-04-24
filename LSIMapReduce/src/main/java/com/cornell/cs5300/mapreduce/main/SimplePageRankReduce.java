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
		Double oldPagerank = 0.0;
		
		System.out.println("----------key from map is -------");
		System.out.println(key);
		
		for(Text val:values) {

			String value = val.toString();

			System.out.println(" Value got from map is " + value);
			
			if (value.contains(Constants.GRAPH_IDENTIFIER)) {
				String vals[] = value.split(" ");

				for (int i = 3; i < vals.length; i++) {
					adjList.append(vals[i]).append(" ");
				}
				outDegree = vals[1];
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
		
		Text reducerKey = new Text();
		reducerKey.set("");

		System.out.println("---------value written by reducer is -------------");
		System.out.println("key = " + reducerKey + "   value = " + reducerOutTxt);
		
		
		context.write(reducerKey, reducerOutTxt);

		double residual = Math.abs(oldPagerank - newPagerank) / newPagerank;
		
		System.out.println("Double residual value is " + residual);
		
		long residualLong = (long) residual * 1000000;
		
		System.out.println("residual value is " + residualLong);
		System.out.println("Initial counter value is " + context.getCounter(Counter.COUNTER));
		
		context.getCounter(Counter.COUNTER).increment(residualLong);
		
		System.out.println(" Counter value after incremeniting is " +  context.getCounter(Counter.COUNTER) );
		

	}
}
