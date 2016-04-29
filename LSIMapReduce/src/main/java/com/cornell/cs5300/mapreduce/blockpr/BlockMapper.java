package com.cornell.cs5300.mapreduce.blockpr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.cornell.cs5300.mapreduce.Util.Constants;
import com.cornell.cs5300.mapreduce.Util.Counter;
import com.cornell.cs5300.mapreduce.Util.Node;
import com.sun.corba.se.impl.javax.rmi.CORBA.Util;

public class BlockMapper extends Mapper<LongWritable, Text, Text, Text> {

	boolean flag = true;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString().trim();

		String split[] = line.split(Constants.DELIMITER);
		String blockid = Node.giveBlockId(split[0].trim());
		String nodeid = Node.giveNodeId(split[0].trim());

		double outdegree = Double.parseDouble(split[1].trim());

		Text mapperKey = new Text();
		mapperKey.set(blockid.trim());

		Text mapperOutput = new Text();

		mapperOutput.set(Constants.GRAPH_IDENTIFIER + Constants.DELIMITER + line);
		// system.out.println("MAPPER EMMITING GRAPH "+ mapperOutput.toString()
		// );
		context.write(mapperKey, mapperOutput);

		for (int j = 3; j < split.length; j++) {
			String adjnodeid = Node.giveNodeId(split[j].trim());
			String adjblockid = Node.giveBlockId(split[j].trim());
			double pagerankTopass = Double.parseDouble(split[2].trim()) / outdegree;

			Text same_block = new Text();
			Text different_block = new Text();

			if (adjblockid.equalsIgnoreCase(blockid)) {

				same_block.clear();

				same_block.set(Constants.SAME_BLOCK_IDENTIFIER + Constants.DELIMITER + nodeid + Constants.DELIMITER
						+ adjnodeid);

				// system.out.println("MAPPER SAME GRAPH "+ "key" +
				// mapperKey.toString() + "VALUE" +same_block.toString());
				context.write(mapperKey, same_block);

			}

			else {

				Text adjblockkey = new Text();
				adjblockkey.set(adjblockid.trim());
				different_block.clear();
				different_block.set(Constants.DIIFERENT_BLOCK_IDENTIFIER + Constants.DELIMITER + nodeid
						+ Constants.DELIMITER + adjnodeid + Constants.DELIMITER + pagerankTopass);
				//// system.out.println("MAPPER DIfferetn GRAPH "+ "key" +
				//// adjblockkey.toString() + "VALUE"
				//// +different_block.toString());
				context.write(adjblockkey, different_block);

			}

		}

	}

}
