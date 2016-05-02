package pagerankmr;

public  class Constants {
	
	public static final double initilaPR = 1 / 685230.0;
	public static String FOLDER_PATH = null;
	public static String EDGES_PATH = FOLDER_PATH + "edges.txt";
	public static String BLOCK_PATH = FOLDER_PATH + "blocks.txt";

	public static boolean isGaussSet = false;

	public static String BLOCK_MODE = null;

	public static String FILTERED_EDGES = FOLDER_PATH + "mgs275_filteredEdges_temp.txt";
	public static String FILTERED_EDGES_DEGREE = FOLDER_PATH + "mgs275_filteredEdges.txt";

	public static final double D_PR = 0.85;
	public static final int N = 685230;

	public static final int MINIMUM = 0;
	public static final int MAXIMUM = 67;

	public static final int BLOCK_SIZE = 68;

	enum Counter {
		COUNTER
	}

	public static final double NET_ID = 0.572; // reverse of digits in mgs275

	public static String RANDOM_IDENTIFIER = "random";

	public static String DELIMITER = " ";
	public static String IDSEPARATOR = ",";

	public static final String GRAPH_IDENTIFIER = "graph";
	public static final String SAME_BLOCK_IDENTIFIER = "BE";
	public static final String DIIFERENT_BLOCK_IDENTIFIER = "BC";

	public static final double ERROR_THRESHHOLD = 0.001;

	public static int numberOfIterations = 50;

}
