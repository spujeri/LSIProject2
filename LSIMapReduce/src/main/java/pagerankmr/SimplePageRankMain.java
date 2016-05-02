package pagerankmr;



/**
 * This is the main class where the MapReduce job is created and iterated until the convergence condition is met. 
 * The convergence condition is tested using the residual value received from the reducer 
 * and checked against the error threshold of 0.001, if residual is less than or equal to this value we break out of the loop and finish.
 *
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimplePageRankMain {

	public static void main(String[] args) throws Exception {

		
		if (args.length < 2) {
			System.out.println("YOU HAVE NOT ENTERED SOME COMMAND LINE ARGUMENTS");
		}
		
		String inputInitial = args[0];
		String outputintial = args[1];
		String output = null;
		int iteration = 1;
		String input = null;
		if (args.length == 3) {
			try {
				Constants.numberOfIterations = Integer.parseInt(args[2]);
			} catch (Exception e) {

				System.out.println("PLEASE ENTER CORRECT NUMBER OF ITERATION");
				return;
			}
		}

		System.out.println("MAXIMUM NUMBER OF IERATION WILL BE PERFORMED IS "+ Constants.numberOfIterations);
		while (iteration <= Constants.numberOfIterations) {

			Job conf = Job.getInstance(new Configuration(), "PageRank");

			if (iteration == 1) {
				input = inputInitial;

			} else {
				input = outputintial + (iteration - 1);
			}
			output = outputintial + iteration;

			conf.setReducerClass(SimplePageRankReduce.class);
			conf.setJarByClass(SimplePageRankMain.class);
			conf.setMapperClass(SimplePageRankMap.class);
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
			conf.setReducerClass(SimplePageRankReduce.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			conf.waitForCompletion(true);

			long longResidual = conf.getCounters().findCounter(Counter.COUNTER).getValue();
			double residual = longResidual / (1000000);
			residual /= Constants.N;

			System.out.println("END OF ITERATION " + iteration + ", RESIDUAL : " + residual);

			if (residual <= Constants.ERROR_THRESHHOLD)
				break;

			// reset residual counter after each iteration
			conf.getCounters().findCounter(Counter.COUNTER).setValue(0L);
			iteration++;
		}

		System.out.println("-----------------------------------------------------------------");
		System.out.println("---------------------End of PageRanking--------------------------");

	}
}