package com.cornell.cs5300.mapreduce.Util;

public abstract class Constants {

	
	//public static final String FILTERED_EDGES = "/Users/Shiva/Documents/Masters/Assignements/LSI/proj2/filteredEdges.txt";
	//public static final String FILTERED_EDGES_DEGREE = "/Users/Shiva/Documents/Masters/Assignements/LSI/proj2/filteredEdgesAdjList.txt";
	public static final double initilaPR = 1/685320.0;
	
	public static final  String EDGES_PATH = "/Users/mihir/Documents/MEng folder new/LSI/Project2/input/edges.txt";
	public static final String BLOCK_PATH = "/Users/mihir/Documents/MEng folder new/LSI/Project2/input/blocks.txt";


	//public static final String FILTERED_EDGES = "/Users/Shiva/Documents/Masters/Assignements/LSI/proj2/filteredEdges.txt";
	//public static final String FILTERED_EDGES_DEGREE = "/Users/Shiva/Documents/Masters/Assignements/LSI/proj2/filteredEdgesAdjList.txt";


	public static final String FILTERED_EDGES = "/Users/mihir/Documents/MEng folder new/LSI/Project2/input/filteredEdgesnewM.txt";
	public static final String FILTERED_EDGES_DEGREE = "/Users/mihir/Documents/MEng folder new/LSI/Project2/input/filteredEdgesAdjListnewM.txt";
	
	

	// public static final String EDGES_PATH = "/Users/mihir/Documents/MEng
	// folder new/LSI/Project2/input/edgestest.txt";
	// public static final String BLOCK_PATH = "/Users/mihir/Documents/MEng
	// folder new/LSI/Project2/input/blocks.txt";

	//public static final String EDGES_PATH = "/Users/Shiva/Documents/Masters/Assignements/LSI/proj2/edges.txt";
	//public static final String BLOCK_PATH = "/Users/Shiva/Documents/Masters/Assignements/LSI/proj2/blocks.txt";


	public static final double D_PR = 0.85;
	public static final int N = 685230;

	enum Counter {
		COUNTER
	}

	
	public static String DELIMITER = " ";
	public static String IDSEPARATOR = ",";
	
	
	public static final String GRAPH_IDENTIFIER = "graph";
	public static final String SAME_BLOCK_IDENTIFIER = "BE";
	public static final String DIIFERENT_BLOCK_IDENTIFIER = "BC";
	
	
	public static final double ERROR_THRESHHOLD = 0.001;
	
	public static final int numberOfIterations = 30;
	
	
	
	
	
}
