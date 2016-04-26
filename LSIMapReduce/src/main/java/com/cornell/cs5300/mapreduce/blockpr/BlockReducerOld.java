package com.cornell.cs5300.mapreduce.blockpr;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cornell.cs5300.mapreduce.Util.Constants;
import com.cornell.cs5300.mapreduce.Util.Counter;
import com.cornell.cs5300.mapreduce.Util.Node;

public class BlockReducerOld extends Reducer<Text, Text, Text, Text> {

	Map<String, Node> blockNodeMap = new HashMap<String, Node>();

	Node createGetNode(String line) {

		String values[] = line.split(" ");
		Node node = null;
		node = blockNodeMap.get(values[0]);
		if (node == null) {
			node = new Node();

			node.setName(values[0]);
			node.setOutDegree(values[1]);
			node.setPageRank(values[2]);
			if (node.getOutDegree() > 0 && values.length > 3) {
				node.setrAdjList(Arrays.copyOfRange(values, 3, values.length));

			}
		} else {
			node.setName(values[0]);
			node.setOutDegree(values[1]);
			node.setPageRank(values[2]);
			if (node.getOutDegree() > 0 && values.length > 3) {
				node.setrAdjList(Arrays.copyOfRange(values, 3, values.length));

			}
		}

		return node;

	}

	String getLine(String grap_line) {

		StringBuilder str = new StringBuilder();
		String words[] = grap_line.trim().split(" ");
		for (int i = 1; i < words.length; i++) {

			str.append(words[i]).append(Constants.DELIMITER);

		}

		return str.toString().trim();

	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// StringBuilder reducerOutput = new
		// StringBuilder(key.toString().trim());
		StringBuilder adjList = new StringBuilder("");
		String outDegree = "0";
		Double newPagerank = 0.0;

		Double oldPagerank = 0.0;
		blockNodeMap.clear();

		// //System.out.println("----------key from map is -------");
		//// System.out.println(key);

		// System.out.println("ITERATOR INSIDE REDUCER" + values);

		for (Text valueTxt : values) {

			String value = valueTxt.toString().trim();

			// System.out.println(" Value got from map is " + value);

			if (value.contains(Constants.GRAPH_IDENTIFIER)) {
				value = getLine(value);
				Node node = createGetNode(value);
				node.setIsSource();
				blockNodeMap.put(node.getName(), node);

			} else if (value.contains(Constants.SAME_BLOCK_IDENTIFIER)) {

				String nodes[] = value.split(" ");
				String nodeID = nodes[2];

				Node node = blockNodeMap.get(nodeID);

				// If the node is only the sink then it needs to considered in
				// PR caluation.
				// as it may not have come as line from mapper
				if (node == null) {

					node = new Node();
					node.setName(nodeID);

				}

				// Check for the same name in map everywhere
				node.addInAddlst(nodes[1]);
				blockNodeMap.put(node.getName(), node);

			} else if (value.contains(Constants.DIIFERENT_BLOCK_IDENTIFIER)) {

				String nodes[] = value.split(" ");
				String nodeID = nodes[2];

				Node node = blockNodeMap.get(nodeID);

				// If the node is only the sink then it needs to considered in
				// PR caluation.
				// as it may not have come as line from mapper
				if (node == null) {

					node = new Node();
					node.setName(nodeID);

				}

				// Check for the same name in map everywhere
				node.setBoundaryPagerank(nodes[3]);
				blockNodeMap.put(node.getName(), node);

			}

		}

		Map<String, Double> startPageRank = new HashMap<String, Double>();

		for (Node node : blockNodeMap.values()) {
			startPageRank.put(node.getName(), node.getPageRank());

		}

		int reduceIteration = 0;
		double resudual = Double.MAX_VALUE;
		while (resudual > Constants.ERROR_THRESHHOLD && reduceIteration <=20) {

			resudual = iterateBlockOnce();
			reduceIteration++;
		}

		for (Node node : blockNodeMap.values()) {
			StringBuilder reducerOutput = new StringBuilder();
			if (node.getIsSource()) {
				reducerOutput.append(node.getName()).append(Constants.DELIMITER).append(node.getOutDegree())
						.append(Constants.DELIMITER).append(node.getPageRank());
				List<String> adjlist = node.getAdjList();

				for (String adjNode : adjlist) {

					reducerOutput.append(Constants.DELIMITER).append(adjNode);
				}

				context.write(new Text(""), new Text(reducerOutput.toString()));

			}

//			System.out.println(
//					"Start Pagerank " + startPageRank.get(node.getName()) + " End PageRank " + node.getPageRank());

			resudual += Math.abs(startPageRank.get(node.getName()) - node.getPageRank()) / node.getPageRank();

		}

		Long residualLong = (long) Math.ceil(resudual) * 1000000;
		context.getCounter(Counter.COUNTER).increment(residualLong);

		// System.out.println(" Counter value after incremeniting is " +
		// context.getCounter(Counter.COUNTER) );

	}

	double iterateBlockOnce() {

		Double residual = 0.0;
		for (Node node : blockNodeMap.values()) {
			double newPageRank = 0.0;

			List<String> inDegreeNodes = node.getIndegreeAdjList();

			if (inDegreeNodes != null) {
				for (String uName : inDegreeNodes) {
					Node nodetmp = blockNodeMap.get(uName);
					// For edges inside the block
					if (nodetmp != null)
					{
						if (nodetmp.getIsSource()) {
							newPageRank += nodetmp.getPageRank() / nodetmp.getOutDegree();
						}

						// Edges coming from outside
						// Boundary page rank is calculated when edge with BE
						// comes
						// for the node
						newPageRank += nodetmp.getBoudaryPagerank();
					}
				}

			}

			newPageRank *= Constants.D_PR;
			newPageRank += (1 - Constants.D_PR) / blockNodeMap.size();

			residual += Math.abs(newPageRank - node.getPageRank()) / newPageRank;
			node.setPageRank(String.valueOf(newPageRank));

		}

		return residual;

	}
}
