package pagerankmr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

public class ParseGraph {
	double fromNetID = Constants.NET_ID; // 82 is 28 reversed
	double rejectMin = 0.9 * fromNetID;
	double rejectLimit = rejectMin + 0.01;
	Map<String, Node> nodeMap = new HashMap<String, Node>();
	List<Integer> listBlock = new ArrayList<Integer>();

	public ParseGraph() {
		try {
			BufferedReader brBlock = new BufferedReader(new FileReader(Constants.BLOCK_PATH));
			String bline = null;
			listBlock.add(Integer.parseInt(brBlock.readLine().trim()) - 1);
			while ((bline = brBlock.readLine()) != null) {
				listBlock.add(Integer.parseInt(bline.trim()) + listBlock.get(listBlock.size() - 1));
			}

			brBlock.close();
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

	/*
	 * int getBlockIdRandom(int node) {
	 * 
	 * Random rn = new Random(); int range = Constants.MAXIMUM -
	 * Constants.MINIMUM + 1; int randomNum = rn.nextInt(range) +
	 * Constants.MINIMUM;
	 * 
	 * return randomNum; }
	 */

	int getBlockIdRandom(int node) {

		int randomID = node % 68;

		return randomID;
	}

	public long filteredEdges = 0;

	public void praseEdges(String random) {
		try {
			BufferedWriter bwr = new BufferedWriter(new FileWriter(Constants.FILTERED_EDGES));
			BufferedReader brEdges = new BufferedReader(new FileReader(Constants.EDGES_PATH));
			Set<String> nodeSet = new HashSet<String>();

			String line = null;

			while ((line = brEdges.readLine()) != null) {
				byte[] bar = line.getBytes();

				String source = new String(Arrays.copyOfRange(bar, 0, 6)).trim();
				String dest = new String(Arrays.copyOfRange(bar, 7, 13)).trim();
				String eValue = new String(Arrays.copyOfRange(bar, 13, 25)).trim();
				String s[] = new String[3];
				s[0] = source;
				s[1] = dest;
				s[2] = eValue;

				nodeSet.add(s[0].trim());

				if (selectInputLine(Double.parseDouble(eValue))) {
					filteredEdges++;
					int sourceInt = Integer.parseInt(source);
					int destInt = Integer.parseInt(dest);
					int sourceBlockId, destBlockId;
					if (random.trim().equals(Constants.RANDOM_IDENTIFIER)) {
						sourceBlockId = getBlockIdRandom(sourceInt);
						destBlockId = getBlockIdRandom(destInt);

					} else {
						sourceBlockId = getBlockId(sourceInt);
						destBlockId = getBlockId(destInt);
					}

					bwr.write(source + " " + sourceBlockId + " " + dest + " " + destBlockId);
					bwr.write("\n");

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

	void computeOutDegree() {

		Map<String, Integer> degreeMap = new HashMap<String, Integer>();
		try {

			BufferedReader brEdges = new BufferedReader(new FileReader(Constants.FILTERED_EDGES));
			BufferedWriter bwr = new BufferedWriter(new FileWriter(Constants.FILTERED_EDGES_DEGREE));

			String line = null;
			while ((line = brEdges.readLine()) != null) {
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

			while ((line = brEdges.readLine()) != null) { // System.out.println(line);
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
				String[] strs = line.split(" ");
				Integer key = Integer.parseInt(strs[0].trim());
				if (degreeMap.containsKey(key)) {

					StringBuilder value = new StringBuilder(degreeMap.get(key)).append(strs[2]).append(",")
							.append(strs[3]).append(" ");
					;
					degreeMap.put(key, value.toString());

				} else {
					StringBuilder str = new StringBuilder();
					str.append(strs[0]).append(",").append(strs[1]).append(" ").append(strs[2]).append(",")
							.append(strs[3]).append(" ");
					degreeMap.put(key, str.toString());
				}

			}

			Iterator<Integer> keysit = degreeMap.keySet().iterator();

			for (int j = 0; j < Constants.N; j++) {
				if (degreeMap.containsKey(j)) {

					String val = degreeMap.get(j);
					String split[] = val.split(" ");

					int outDegree = split.length - 1;

					if (outDegree == 0)
						System.out.println(split[0]);

					StringBuilder newVal = new StringBuilder(split[0]).append(" ").append(String.valueOf(outDegree))
							.append(" ").append(String.valueOf(Constants.initilaPR));
					for (int i = 1; i < split.length; i++)
						newVal.append(" ").append(split[i]);

					System.out.println(newVal);
					bwr.write(newVal.toString());
					bwr.write("\n");

				}

				else {

					int sourceBlockId = getBlockId(j);
					StringBuilder newVal = new StringBuilder(
							String.valueOf(j) + Constants.IDSEPARATOR + String.valueOf(sourceBlockId)).append(" ")
									.append(0).append(" ").append(String.valueOf(Constants.initilaPR));
					bwr.write(newVal.toString());
					bwr.write("\n");
				}

			} // end of for loop

			bwr.close();
			brEdges.close();
		} catch (Exception e) {

		}

	}

	public static void main(String a[]) {

		if (a.length < 1) {
			System.out.println("Pass required arguments in command line as specified in README.txt");
		}

		Constants.FOLDER_PATH = a[0];
		Constants.EDGES_PATH = Constants.FOLDER_PATH + File.separator + "edges.txt";
		Constants.BLOCK_PATH = Constants.FOLDER_PATH + File.separator + "blocks.txt";

		Constants.FILTERED_EDGES = Constants.FOLDER_PATH + File.separator + "mgs275_filteredEdges_temp.txt";
		Constants.FILTERED_EDGES_DEGREE = Constants.FOLDER_PATH + File.separator + "mgs275_filteredEdges.txt";
		ParseGraph pgraph = new ParseGraph();

		if (a.length == 2) {
			if (!a[1].equals("random")) {
				System.out.println("Enter command line argument as random");
			} else {

				pgraph.praseEdges(a[1]);

			}
		} else {
			pgraph.praseEdges("");
		}

		pgraph.computeAdjList();
		System.out.println("TOTAL NUMBER OF EDGES SELECTED " + pgraph.filteredEdges);
	}

}
