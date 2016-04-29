package com.cornell.cs5300.mapreduce.gaussssiedel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cornell.cs5300.mapreduce.Util.Constants;
import com.cornell.cs5300.mapreduce.Util.Counter;
import com.sun.corba.se.impl.orbutil.closure.Constant;


public class BlockMain {

	public static void main(String args[]) {

		System.out.println("ARG0 " + args[0]);
		System.out.println("ARG1 " + args[1]);
		String inputInitial = args[0];
		String outputintial = args[1];
		String output = null;
		int iteration = 1;
		String input = null;
		StringBuilder outStr = new StringBuilder();
		while (iteration <= Constants.numberOfIterations) {

			try {
			

				if (iteration == 1) {
					input = inputInitial;

				} else {
					input = outputintial + (iteration - 1);
				}
				output = outputintial + iteration;

				// conf.setCombinerClass(SimplePageRankReduce.class);

				Job conf = createJobConf(input, output, iteration);
			
				
				conf.waitForCompletion(true);
				
				
				
				long longResidual = conf.getCounters().findCounter(Counter.COUNTER).getValue();
				double residual = longResidual / (1000000);
				residual /= Constants.N;

				System.out.println("--------------------Inside Iteration----------------");
				System.out.println("Iternation number " + iteration + " residual " + residual);

				if (residual <= Constants.ERROR_THRESHHOLD)
					break;

				// reset residual counter after each iteration
				conf.getCounters().findCounter(Counter.COUNTER).setValue(0L);
				
				conf.getCounters().findCounter(Counter.COUNTER).setValue(iteration);
				
				iteration++;
			}

			catch (Exception e) {

			}

			System.out.println("-----------------------------------------------------------------");
			System.out.println("---------------------End of PageRanking--------------------------");
			System.out.println("Iteration number " + iteration);

		}

	}  // end of main
	
	
	public static Job createJobConf(String input,String output, int iteration)
	
	{
		
		Job conf = null;
		try
		{
		Configuration jobConfig = new JobConf();
		jobConfig.setInt("Iteration", 1);
		conf = Job.getInstance(jobConfig, "BlockPageRank");
	
		conf.setJarByClass(BlockMain.class);
		conf.setMapperClass(BlockMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setReducerClass(BlockReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		return conf;
		
		
		}
		catch(Exception e )
		{
			e.printStackTrace();
		}
		
		
		return conf;
	}
	
	
	
	

}
