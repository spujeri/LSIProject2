package com.cornell.cs5300.mapreduce.blockpr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.cornell.cs5300.mapreduce.Util.Constants;

public class BlockMapper extends Mapper<LongWritable, Text, Text, Text> {

	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString().trim();
		
		String split[] = line.split(Constants.DELIMITER);
		String blockid = giveBlockId(split[0]);
		String nodeid = giveNodeId(split[0]);
		
		double outdegree = Double.parseDouble(split[1]);
		
		Text mapperKey = new Text();
		mapperKey.set(blockid.trim());
		
		Text mapperOutput = new Text();
		mapperOutput.set( Constants.GRAPH_IDENTIFIER + line);
		
		context.write(mapperKey,mapperOutput);
		
		for(int j=3 ; j < split.length; j ++ )
		{
			String adjnodeid = giveNodeId(split[j]);
			String adjblockid = giveBlockId(split[j]);
			double pagerankTopass = Double.parseDouble(split[2]) / outdegree;
			
			Text same_block = new Text();
			Text different_block = new Text();
			
			
			if(adjblockid.equalsIgnoreCase(blockid))
			{
				
			same_block.clear();
			same_block.set(Constants.SAME_BLOCK_IDENTIFIER + Constants.DELIMITER + nodeid + Constants.DELIMITER + adjnodeid);
			context.write(mapperKey, same_block );
				
			}
			
			else
			{
			
				Text adjblockkey = new Text();
				adjblockkey.set(adjblockid.trim());
				different_block.clear();
				different_block.set(Constants.DIIFERENT_BLOCK_IDENTIFIER + Constants.DELIMITER +  nodeid + Constants.DELIMITER + adjnodeid + Constants.DELIMITER + pagerankTopass	 );
				context.write(adjblockkey, different_block);
				
				
			}
			
		}
		
	}
	
	
	public static String giveNodeId(String s)
	{
		return s.substring(0, s.indexOf(Constants.IDSEPARATOR));
		
	}
	
	public static String giveBlockId(String s)
	{
		return s.substring(s.indexOf(Constants.IDSEPARATOR),s.length() );
	}
	
	
	
	
}
