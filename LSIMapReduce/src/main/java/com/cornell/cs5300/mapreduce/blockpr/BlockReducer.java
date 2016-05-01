package com.cornell.cs5300.mapreduce.blockpr;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cornell.cs5300.mapreduce.Util.Constants;
import com.cornell.cs5300.mapreduce.Util.Counter;
import com.cornell.cs5300.mapreduce.Util.Node;

public class BlockReducer extends Reducer<Text, Text, Text, Text> {

	Map<String, Node> blockNodeMap = new HashMap<String, Node>();

	Map<String, List<String>> BEMap = new HashMap<String, List<String>>();
	Map<String, Double> BCMap = new HashMap<String, Double>();
	Map<String, Double> nprMap = new HashMap<String, Double>();

	Node createGetNode(String line) {

		String values[] = line.split(" ");
		Node node = null;
		node = blockNodeMap.get(values[0]);
		if (node == null) {
			node = new Node();
			String nodeName = Node.giveNodeId(values[0].trim());
			node.setName(nodeName);
			node.setOutDegree(values[1]);
			node.setPageRank(values[2]);
			if (node.getOutDegree() > 0 && values.length > 3) {
				node.setrAdjList(Arrays.copyOfRange(values, 3, values.length));

			}
		} else {
			String nodeName = Node.giveNodeId(values[0].trim());
			node.setName(nodeName);
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

		int minNode1 = Integer.MAX_VALUE, minNode2 = Integer.MAX_VALUE;
		blockNodeMap.clear();
		BEMap.clear();
		BCMap.clear();
		nprMap.clear();

		// ////system.out.println("----------key from map is -------");
		//// //system.out.println(key);

		// //system.out.println("ITERATOR INSIDE REDUCER" + values);

		for (Text valueTxt : values) {

			String value = valueTxt.toString().trim();

			// //system.out.println(" Value got from map is " + value);

			if (value.contains(Constants.GRAPH_IDENTIFIER)) {
				value = getLine(value);
				// //system.out.println("New line after removing the graph
				// prefix
				// " + value);
				Node node = createGetNode(value);
				node.setIsSource();
				blockNodeMap.put(node.getName(), node);

			} else if (value.trim().contains(Constants.SAME_BLOCK_IDENTIFIER)) {
				value = value.trim();
				String nodes[] = value.split(" ");
				String nodeV = nodes[2];
				String nodeU = nodes[1];
				List<String> uLst = BEMap.get(nodeV);
				if (uLst == null) {
					uLst = new ArrayList<String>();

				}

				//// system.out.println("Inside same block node v" +nodeV
				//// +"list"+uLst);
				Node node = blockNodeMap.get(nodeV);

				// If the node is only the sink then it needs to considered in
				// PR caluation.
				// as it may not have come as line from mapper
				if (node == null) {

					node = new Node();
					node.setName(nodeV);

				}

				uLst.add(nodeU);
				BEMap.put(nodeV, uLst);
				blockNodeMap.put(nodeV, node);

			} else if (value.trim().contains(Constants.DIIFERENT_BLOCK_IDENTIFIER)) {

				value = value.trim();
				String nodes[] = value.split(" ");
				String nodeV = nodes[2];
				// String nodeU = nodes[1];
				Double bcPr = 0.0;
				;

				if (BCMap.containsKey(nodeV)) {
					bcPr = BCMap.get(nodeV);
				} else {
					bcPr = 0.0;
				}
				//// system.out.println("Inside different block node v "+ nodeV
				//// + " node u" +nodeU);
				Node node = blockNodeMap.get(nodeV);

				// If the node is only the sink then it needs to considered in
				// PR caluation.
				// as it may not have come as line from mapper
				if (node == null) {

					node = new Node();
					node.setName(nodeV);

				}
				bcPr += Double.parseDouble(nodes[3]);
				BCMap.put(nodeV, bcPr);
				blockNodeMap.put(nodeV, node);

			}

		}
		// system.out.println("BC Map" + BCMap);
		// system.out.println("BE MAP " + BEMap);
		Map<String, Double> startPageRank = new HashMap<String, Double>();

		for (Node node : blockNodeMap.values()) {
			startPageRank.put(node.getName(), node.getPageRank());
			int nodeInt = Integer.parseInt(node.getName());
			if (nodeInt < minNode1) {
				minNode1 = nodeInt;
			} else if (nodeInt < minNode2) {

				minNode2 = nodeInt;
			}
			//// system.out.println("Node name " + node.getName() + " List " +
			//// BEMap.get(node.getName()));

		}

		int reducePrIteration = 1;
		double resudual = Double.MAX_VALUE;
		// while (resudual > Constants.ERROR_THRESHHOLD && reducePrIteration <=
		// 5) {
		while (resudual > Constants.ERROR_THRESHHOLD) {

			resudual = iterateBlockOnce(context);
			reducePrIteration++;
		}

		double resudualError = 0.0;

		for (Node node : blockNodeMap.values()) {
			StringBuilder reducerOutput = new StringBuilder();
			// if (node.getIsSource()) {
			reducerOutput.append(node.getName() + Constants.IDSEPARATOR + key.toString().trim())
					.append(Constants.DELIMITER).append(node.getOutDegree()).append(Constants.DELIMITER)
					.append(nprMap.get(node.getName()));
			List<String> adjlist = node.getAdjList();

			for (String adjNode : adjlist) {

				reducerOutput.append(Constants.DELIMITER).append(adjNode);
			}

			context.write(new Text(""), new Text(reducerOutput.toString().trim()));

			// }

			// //system.out.println(
			// "Start Pagerank " + startPageRank.get(node.getName()) + "
			// EndPageRank " + node.getPageRank());

			resudualError += Math.abs(startPageRank.get(node.getName()) - nprMap.get(node.getName()))
					/ nprMap.get(node.getName());
			/*
			 * System.out.println("Start Pagerank " +
			 * startPageRank.get(node.getName()) + " EndPageRank " +
			 * nprMap.get(node.getName())+ " residual " + resudualError);
			 */
		}

		Long residualLong = (long) Math.ceil(resudualError) * 1000000;
		context.getCounter(Counter.COUNTER).increment(residualLong);

		// //system.out.println(" Counter value after incremeniting is " +

		// context.getCounter(Counter.COUNTER) );
		System.out.println("BLOCK " + key.toString() + " MIN_NODE1 " + minNode1 + " PR "
				+ blockNodeMap.get(String.valueOf(minNode1)).getPageRank() + " MIN_NODE2 " + minNode2 + " PR "
				+ blockNodeMap.get(String.valueOf(minNode2)).getPageRank());
		cleanup(context);
		
		

	}

	double iterateBlockOnce(Reducer.Context context) {

		// system.out.println("WHOLE BLOCK GRAPH "+ blockNodeMap);
		Double residual = 0.0;
		Set<String> nodeKeys = blockNodeMap.keySet();
		nprMap.clear();
		for (String nodeV : nodeKeys) {
			double newPageRank = 0.0;
			Node node = blockNodeMap.get(nodeV);
			// String nodeV = node.getName();
			List<String> inDegreeNodes = BEMap.get(nodeV);
			// system.out.println("Node " + nodeV + "List " + inDegreeNodes);
			if (inDegreeNodes != null) {
				for (String uName : inDegreeNodes) {
					Node nodetmp = blockNodeMap.get(uName);
					// For edges inside the blhetock
					// if (nodetmp.getIsSource()) {
					// system.out.println("REDUCER MR Node "+node.getName() + "
					// PAGERANK" + nodetmp.getPageRank() +
					// "OUTDEGREE"+nodetmp.getOutDegree());
					newPageRank += nodetmp.getPageRank() / nodetmp.getOutDegree();
					// }

					// Edges coming from outside
					// Boundary page rank is calculated when edge with BE
					// comes
					// for the node

				}

			}

			if (BCMap.containsKey(nodeV))
				newPageRank += BCMap.get(nodeV);

			newPageRank *= Constants.D_PR;
			newPageRank += (1 - Constants.D_PR) / Constants.N;

			residual += Math.abs(newPageRank - node.getPageRank()) / newPageRank;
			nprMap.put(nodeV, newPageRank);

		}
		context.getCounter(Counter.AVG_ITERATION).increment(1);
		for (Node node : blockNodeMap.values()) {

			node.setPageRank(String.valueOf(nprMap.get(node.getName())));
		}

		// System.out.println("RESIDULA INSIDE LOCAL MAP REDUCE " + residual);
		return residual / blockNodeMap.size();

	}
}
