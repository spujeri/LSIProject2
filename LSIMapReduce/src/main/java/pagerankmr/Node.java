package pagerankmr;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.*;

public class Node implements Comparable<Node> {

	List<String> inAdjList = new LinkedList<String>();
	List<Node> outAdjList = new LinkedList<Node>();

	Set<Node> inDegreeSet = new HashSet<Node>();
	Set<Node> outDegreeSet = new HashSet<Node>();

	List<String> outAdjListString = new LinkedList<String>();

	String name;
	float prValue;
	String blockNumber;
	int degree;
	double parank;
	double boudaryPageRank = 0.0;
	boolean isSource = false;
	boolean inSameBlock;

	public void setBoundaryPagerank(String prval) {

		Double pr = Double.parseDouble(prval);
		boudaryPageRank += pr;
	}

	public double getBoudaryPagerank() {

		return boudaryPageRank;
	}

	public Node(String name) {
		// TODO Auto-generated constructor stub
		this.name = name;
	}

	// To check node is Source in Block or not
	public void setIsSource() {

		isSource = true;
	}

	public boolean getIsSource() {

		return isSource;

	}

	public Node() {

	}

	public void setPageRank(String pgrk) {

		parank = Double.parseDouble(pgrk);
	}

	public double getPageRank() {
		return parank;
	}

	public void addNeighbourOut(Node node) {

		if (!outDegreeSet.contains(node)) {
			outDegreeSet.add(node);
			outAdjList.add(node);

		}

	}

	public void setOutDegree(String degree) {

		this.degree = Integer.parseInt(degree);

	}

	public void setName(String name) {

		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setBlock(String block) {

		this.blockNumber = block;
	}

	public String getBlock() {
		return blockNumber;
	}

	public void setrAdjList(String aL[]) {

		for (String node : aL) {
			outAdjListString.add(node);

		}

	}

	public int getOutDegree() {

		return degree;
	}

	public List<String> getAdjList() {

		return outAdjListString;
	}

	public void addNeighbourOut(String dest) {
		Node node = new Node(dest);
		addNeighbourOut(node);

	}
	
	
	public static String giveNodeId(String s)
	{
		
		try
		{
		return s.substring(0, s.indexOf(Constants.IDSEPARATOR));
		}
		catch(Exception e)
		{
			System.out.println("NODE WITH BLOCK=" + s);
			e.printStackTrace();
			return null;
		}
		
	}
	
	public static String giveBlockId(String s)
	{
		return s.substring(s.indexOf(Constants.IDSEPARATOR)+1,s.length() );
	}

	public void addInAddlst(String str) {

		inAdjList.add(str);
	}

	public List<String> getIndegreeAdjList() {

		return inAdjList;
	}

	public int hashCode() {
		int sum = 0;
		char c[] = name.toCharArray();
		for (char a : c) {
			sum += a;
		}
		return sum;
	}

	public boolean equals(Object obj) {
		if (this.name.equals(obj)) {
			return true;
		}
		return false;
	}

	public int compareTo(Node o) {
		if (this.name.equals(o.name)) {
			return 0;
		} else if (!this.name.equals(o.name)) {
			return -1;
		} else {
			return 1;
		}
	}

}
