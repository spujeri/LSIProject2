package com.cornell.cs5300.mapreduce.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

public class ParseGraph {
	double fromNetID = 0.9332; // 82 is 28 reversed
	double rejectMin = 0.9 * fromNetID;
	double rejectLimit = rejectMin + 0.01;
	Map<String, Node> nodeMap = new HashMap<String, Node>();
	List<Integer> listBlock = new ArrayList<Integer>();

	public ParseGraph() {
		// TODO Auto-generated constructor stub

		try {
			BufferedReader brBlock = new BufferedReader(new FileReader(Constants.BLOCK_PATH));
			String bline = null;
			listBlock.add(Integer.parseInt(brBlock.readLine().trim()) - 1);
			while ((bline = brBlock.readLine()) != null) {
				listBlock.add(Integer.parseInt(bline.trim()) + listBlock.get(listBlock.size() - 1));
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

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

	int getBlockId(int node) {
		int tmpBlockId = node / 10000;
		if (tmpBlockId > 0)
			tmpBlockId--;

		while (true) {

			if (listBlock.get(tmpBlockId) < node) {
				tmpBlockId++;
				return tmpBlockId;

			} else {
				return tmpBlockId;
			}
		}

	}

	public void praseEdges() {
		try {

			// brBlock = new BufferedReader(new
			// FileReader(Constants.BLOCK_PATH));
			BufferedWriter bwr = new BufferedWriter(new FileWriter(Constants.FILTERED_EDGES));
			BufferedReader brEdges = new BufferedReader(new FileReader(Constants.EDGES_PATH));

			String line = null;
			while ((line = brEdges.readLine()) != null) {
				// System.out.println(line);

				String[] strs = line.split(" ");
				int i = 0;
				byte[] bar = line.getBytes();
				// Arrays.c
				String source = new String(Arrays.copyOfRange(bar, 0, 6)).trim();
				String dest = new String(Arrays.copyOfRange(bar, 7, 13)).trim();
				String eValue = new String(Arrays.copyOfRange(bar, 13, 25)).trim();
				String s[] = new String[3];

				System.out.println("source " + source + " dest " + dest + " edgeValue " + eValue);
				s[0] = source;
				s[1] = dest;
				s[2] = eValue;

				if (selectInputLine(Double.parseDouble(eValue))) {
					int sourceInt = Integer.parseInt(source);
					int destInt = Integer.parseInt(dest);
					int sourceBlockId = getBlockId(sourceInt);
					int destBlockId = getBlockId(destInt);
					// sys
					bwr.write(source + " " + sourceBlockId + " " + dest + " " + destBlockId);
					bwr.write("\n");

					// addToMAp(s);
				}

			}
			brEdges.close();
			bwr.close();
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

	void computeOutDegree() {

		Map<String, Integer> degreeMap = new HashMap<String, Integer>();
		try {

			BufferedReader brEdges = new BufferedReader(new FileReader(Constants.FILTERED_EDGES));
			BufferedWriter bwr = new BufferedWriter(new FileWriter(Constants.FILTERED_EDGES_DEGREE));
			
			String line = null;
			while ((line = brEdges.readLine()) != null) {
				// System.out.println(line);

				String[] strs = line.split(" ");
				if (degreeMap.containsKey(strs[0])) {
					int value = degreeMap.get(strs[0]) + 1;
					degreeMap.put(strs[0], value);

				} else {
					degreeMap.put(strs[0], 1);
				}

			}
			brEdges.close();
			brEdges = new BufferedReader(new FileReader(Constants.FILTERED_EDGES));

			while ((line = brEdges.readLine()) != null) {
				// System.out.println(line);
				line = line + " " + degreeMap.get(line.split(" ")[0]);
				bwr.write(line);
				bwr.write("\n");

			}

			bwr.close();
			brEdges.close();
		} catch (Exception e) {

		}

	}

	void computeAdjList() {

		Map<Integer, String> degreeMap = new TreeMap<Integer, String>();
		try {

			BufferedWriter bwr = new BufferedWriter(new FileWriter(Constants.FILTERED_EDGES_DEGREE));
			BufferedReader brEdges = new BufferedReader(new FileReader(Constants.FILTERED_EDGES));
			String line = null;
			while ((line = brEdges.readLine()) != null) {
				// System.out.println(line);

				String[] strs = line.split(" ");
				Integer key = Integer.parseInt( strs[0].trim());
				if (degreeMap.containsKey(key)) {
					
					StringBuilder value = new StringBuilder(degreeMap.get(key)).append(strs[2]).append(",")
							.append(strs[3]).append(" ");;
					degreeMap.put(key, value.toString());

				} else {
					StringBuilder str = new StringBuilder();
					str.append(strs[0]).append(",").append(strs[1]).append(" ").append(strs[2]).append(",")
							.append(strs[3]).append(" ");
					degreeMap.put(key, str.toString());
				}

			}

			
			Iterator< Integer> keysit = degreeMap.keySet().iterator(); 
			while (keysit.hasNext()) {
				// System.out.println(line);

				String val = degreeMap.get(keysit.next());
				String split[] = val.split(" ");
			
				int outDegree = split.length - 1 ;
				StringBuilder newVal = new StringBuilder(split[0]).append(" ").append(String.valueOf(outDegree)).append(" ").append(String.valueOf(Constants.initilaPR));
				for(int i = 1; i<split.length; i++)
					newVal.append(" ").append(split[i]);
				
			//	String vale= val+ String.valueOf(Constants.initilaPR);
				System.out.println(newVal);
				bwr.write(newVal.toString());
				bwr.write("\n");

			}

			bwr.close();
			brEdges.close();
		} catch (Exception e) {

		}

	}

	public static void main(String a[]) {

		ParseGraph pgraph = new ParseGraph();
		System.out.println(pgraph.listBlock);
	//	 pgraph.praseEdges();
		// pgraph.dispNodes();
		pgraph.computeAdjList();
	}

}
