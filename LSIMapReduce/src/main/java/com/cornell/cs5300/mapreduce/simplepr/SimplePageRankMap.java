package com.cornell.cs5300.mapreduce.simplepr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cornell.cs5300.mapreduce.Util.Constants;

public class SimplePageRankMap extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString().trim();

		
		//System.out.println("-------input to mapper------");
		//System.out.println(line);

		Text str = new Text();
		str.set(Constants.GRAPH_IDENTIFIER + line);
		String out[] = line.split(" ");
		Text keyNode = new Text();
		keyNode.set(out[0].trim());
		context.write(keyNode, str);
		
		//System.out.println("------------ graph value written by map is -------");
		//System.out.println("key = " + keyNode + "  Value=" + str);
		
		Double pagerankVal = Double.parseDouble((out[2]));

		int outDegree = Integer.parseInt(out[1]);
		if (outDegree > 0 && out.length > 3) {
			double pgrval = pagerankVal / outDegree;
			for (int i = 3; i < out.length; i++) {
				Text pgrank = new Text();
				pgrank.set(String.valueOf(pgrval));
				Text reducesKey = new Text();
				reducesKey.set(out[i].trim());
				
				//System.out.println("------------ Pagerank value written by map is -------");
				//System.out.println("key = " + reducesKey + "  Value = " + pgrank);
				
				context.write(reducesKey, pgrank);

			}
		}

	}

}