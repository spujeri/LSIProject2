package pagerankmr;

/**
 * This is the mapper class of the simple page rank implementation, the contents of “mgs275_filteredEdges.txt” file are passed as the input. 
 * From the input the source node is extracted and then the mapper class emits the same line as the output with the source
   node as the key, this is to retain the graph structure. Then the mapper parses the rest of the input
   to extract destination nodes from the adjacency list and the page rank. The mapper finally emits
   the destination node and PageRank divided by the outdegree of source node.
 * 
 * 
 */


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


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
