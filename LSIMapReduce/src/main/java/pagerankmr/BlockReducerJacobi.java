package pagerankmr;

/**
 * 
 * It is a blocked reducer class for jacobi implementation.
 * It computes the page rank of each node in the block as per the iterateBlockOnce pesudo code given in instructions.
 * It constructs the suitable hashmap based on the input from mapper.
 * Boundary page ranks are directly summed up , whereas for nodes in same block page rank is divided by outdegree of source node and then add.
 * For detailed information, refer ReadMe
 * 
 */


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BlockReducerJacobi extends Reducer<Text, Text, Text, Text> {

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

		int minNode1 = Integer.MAX_VALUE, minNode2 = Integer.MAX_VALUE;
		blockNodeMap.clear();
		BEMap.clear();
		BCMap.clear();
		nprMap.clear();

		for (Text valueTxt : values) {

			String value = valueTxt.toString().trim();

			if (value.contains(Constants.GRAPH_IDENTIFIER)) {
				value = getLine(value);
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

				Node node = blockNodeMap.get(nodeV);

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
				Double bcPr = 0.0;
				;

				if (BCMap.containsKey(nodeV)) {
					bcPr = BCMap.get(nodeV);
				} else {
					bcPr = 0.0;
				}

				Node node = blockNodeMap.get(nodeV);

				if (node == null) {

					node = new Node();
					node.setName(nodeV);

				}
				bcPr += Double.parseDouble(nodes[3]);
				BCMap.put(nodeV, bcPr);
				blockNodeMap.put(nodeV, node);

			}

		}

		Map<String, Double> startPageRank = new HashMap<String, Double>();

		for (Node node : blockNodeMap.values()) {
			startPageRank.put(node.getName(), node.getPageRank());
			int nodeInt = Integer.parseInt(node.getName());
			if (nodeInt < minNode1) {
				minNode1 = nodeInt;
			} else if (nodeInt < minNode2) {

				minNode2 = nodeInt;
			}
		}

		int reducePrIteration = 1;
		double resudual = Double.MAX_VALUE;

		while (resudual > Constants.ERROR_THRESHHOLD) {

			resudual = iterateBlockOnceJacobi(context);

			reducePrIteration++;
		}

		double resudualError = 0.0;

		for (Node node : blockNodeMap.values()) {
			StringBuilder reducerOutput = new StringBuilder();

			// Emit the updated page rank of node
			reducerOutput.append(node.getName() + Constants.IDSEPARATOR + key.toString().trim())
					.append(Constants.DELIMITER).append(node.getOutDegree()).append(Constants.DELIMITER)
					.append(nprMap.get(node.getName()));
			List<String> adjlist = node.getAdjList();

			for (String adjNode : adjlist) {

				reducerOutput.append(Constants.DELIMITER).append(adjNode);
			}

			context.write(new Text(""), new Text(reducerOutput.toString().trim()));

			resudualError += Math.abs(startPageRank.get(node.getName()) - nprMap.get(node.getName()))
					/ nprMap.get(node.getName());

		}

		Long residualLong = (long) Math.ceil(resudualError) * 1000000;
		context.getCounter(Counter.COUNTER).increment(residualLong);

		System.out.println("BLOCK " + key.toString() + " MIN_NODE1 " + minNode1 + " PR "
				+ blockNodeMap.get(String.valueOf(minNode1)).getPageRank() + " MIN_NODE2 " + minNode2 + " PR "
				+ blockNodeMap.get(String.valueOf(minNode2)).getPageRank());
		cleanup(context);

	}

	double iterateBlockOnceJacobi(Context context) {

		Double residual = 0.0;
		Set<String> nodeKeys = blockNodeMap.keySet();
		nprMap.clear();
		for (String nodeV : nodeKeys) {
			double newPageRank = 0.0;
			Node node = blockNodeMap.get(nodeV);

			List<String> inDegreeNodes = BEMap.get(nodeV);

			if (inDegreeNodes != null) {
				for (String uName : inDegreeNodes) {
					Node nodetmp = blockNodeMap.get(uName);
					newPageRank += nodetmp.getPageRank() / nodetmp.getOutDegree();
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
		return residual / blockNodeMap.size();

	}

}
