package pagerankmr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BlockMain {

	public static void main(String args[]) {

		if (args.length < 3) {
			System.out.println("YOU HAVE NOT ENTERED SOME COMMAND LINE ARGUMENTS");
			return;
		}
		if(args.length == 4){
			
			try{
			Constants.numberOfIterations = Integer.parseInt(args[3]);
			}
			catch(Exception e){
				
				System.out.println("PLEASE ENTER CORRECT NUMBER OF ITERATION");
				return;
			}
		}
		
		System.out.println("MAXIMUM NUMBER OF IERATION WILL BE EXECUTED IS "+ Constants.numberOfIterations);
		
		String inputInitial = args[0];
		String outputintial = args[1];

		String output = null;
		int iteration = 1;
		String input = null;

		while (iteration <= Constants.numberOfIterations) {

			try {

				if (iteration == 1) {
					input = inputInitial;

				} else {
					input = outputintial + (iteration - 1);
				}
				output = outputintial + iteration;

				Job conf = createJobConf(input, output, iteration, args[2]);
				if (conf == null) {
					return;
				}
				conf.waitForCompletion(true);

				long longResidual = conf.getCounters().findCounter(Counter.COUNTER).getValue();
				double residual = longResidual / (1000000);
				residual /= Constants.N;
				System.out.println("\n");
				
				System.out.println("END OF ITERATION " + iteration + ", RESIDUAL :" + residual);
				double averg_Iteration =  conf.getCounters().findCounter(Counter.AVG_ITERATION).getValue()*1.0
						/ Constants.BLOCK_SIZE;
	
				System.out.println("AVERAGE NUMBER OF ITERATIONS IN REDUCER = " + averg_Iteration);
				
				System.out.println("\n");
				conf.getCounters().findCounter(Counter.COUNTER).setValue(0L);

				if (residual <= Constants.ERROR_THRESHHOLD) {
					break;
				}

				conf.getCounters().findCounter(Counter.AVG_ITERATION).setValue(0L);

				iteration++;
			}

			catch (Exception e) {

			}

		}
		System.out.println("-----------------------------------------------------------------");
		System.out.println("---------------------END OF PAGERANKING--------------------------");
		System.out.println("===================================================================");

	}

	public static Job createJobConf(String input, String output, int iteration, String flag)

	{

		Job conf = null;
		try {
			Configuration jobConfig = new JobConf();
			jobConfig.setInt("Iteration", 1);
			conf = Job.getInstance(jobConfig, "BlockPageRank");

			conf.setJarByClass(BlockMain.class);
			conf.setMapperClass(BlockMapper.class);
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
			if (flag.trim().equalsIgnoreCase(("jacobi"))) {
				conf.setReducerClass(BlockReducerJacobi.class);
			} else if (flag.trim().equalsIgnoreCase(("gauss"))) {
				conf.setReducerClass(BlockReducerGauss.class);
				System.out.println("JACOBI BLOCK PAGERANK WILL RUN");
			} else {
				System.out.println("Please mention right alogorithm name in command line arguement");
				return null;
			}
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			return conf;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return conf;
	}

}
