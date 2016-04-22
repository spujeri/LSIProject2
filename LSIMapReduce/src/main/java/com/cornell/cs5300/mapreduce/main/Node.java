package com.cornell.cs5300.mapreduce.main;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.*;

public class Node implements Comparable<Node> {

	List<Node> inAdjList = new LinkedList<Node>();
	List<Node> outAdjList = new LinkedList<Node>();

	Set<Node> inDegreeSet = new HashSet<Node>();
	Set<Node> outDegreeSet = new HashSet<Node>();

	String name;
	float prValue;
	String blockNumber;

	public Node(String name) {
		// TODO Auto-generated constructor stub
		this.name = name;
	}

	public Node() {

	}

	void addNeighbourOut(Node node) {

		if (!outDegreeSet.contains(node)) {
			outDegreeSet.add(node);
			outAdjList.add(node);

		}

	}

	void addNeighbourOut(String dest) {
		Node node = new Node(dest);
		addNeighbourOut(node);

	}

	void addNeighbouIn(String str[]) {

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
