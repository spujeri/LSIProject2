package com.cornell.cs5300.mapreduce.main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;


public class ParseGraph {
	double fromNetID = 0.93; // 82 is 28 reversed
	double rejectMin = 0.9 * fromNetID;
	double rejectLimit = rejectMin + 0.01;
	Map<String, Node> nodeMap = new HashMap<String, Node>();

	void addToMAp(String s[]) {
		String source = s[0];
		String dest = s[1];
		if (nodeMap.containsKey(source)) {
			Node node = nodeMap.get(source);
			node.addNeighbourOut(dest);

		} else {
			Node node = new Node();
			node.name = source;
			node.addNeighbourOut(dest);
			nodeMap.put(source, node);
		}
	}

	public void praseEdges() {
		try {

			BufferedReader br = new BufferedReader(new FileReader(Constants.EDGES_PATH));
			String line = null;
			while ((line = br.readLine()) != null) {
				//System.out.println(line);

				String[] strs = line.split(" ");
				int i = 0;
				byte[] bar = line.getBytes();
				//Arrays.c
				String source = new String(Arrays.copyOfRange(bar, 0, 6)).trim();
				String dest = new String(Arrays.copyOfRange(bar, 7, 13)).trim();
				String eValue = new String(Arrays.copyOfRange(bar, 13, 25)).trim();
				String s[] = new String[3];

				System.out.println("source " +source + " dest " + dest + " edgeValue " + eValue);
				s[0] = source;
				s[1] = dest;
				s[2] = eValue;

				if (selectInputLine(Double.parseDouble(eValue))) {
					addToMAp(s);
				}

			}

		} catch (Exception e) {
			e.printStackTrace();

		}

	}

	boolean selectInputLine(double x) {
		return (((x >= rejectMin) && (x < rejectLimit)) ? false : true);
	}

	void dispNodes() {

		Set<String> keys = nodeMap.keySet();

		for (String key : keys) {
			System.out.println("Node name " + nodeMap.get(key).name);
		}

	}

	public static void main(String a[]) {

		ParseGraph pgraph = new ParseGraph();
		pgraph.praseEdges();
		pgraph.dispNodes();
	}

}
