import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/** 
 * You can modify this class as you see fit, as long as you correctly update the
 * global centroids.
 */
public class ClusterToPointReducer extends Reducer<Text, Text, Text, Text>
{
	private int counter=0;
	public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
            ArrayList<Point> pointsincluster = new ArrayList<Point>();
         	int position=findPosition(key);
            
            // copy the Iterable list of points (of Text format) into the ArrayList of Points
            
            for (Text p : values)
            {
            	pointsincluster.add(new Point(p.toString()));
            }
            
            Point new_centroid=new Point(pointsincluster.get(0));
            
            //calculate the new centroid

            for (int i=0; i<pointsincluster.get(0).getDimension();i++)
            {
            	float temp_sum=0;
            	for (int j=0; j< pointsincluster.size();j++)
            		temp_sum+= pointsincluster.get(j).cordinates[i];
            	new_centroid.cordinates[i]= temp_sum/pointsincluster.size();
            }
           
            //update the globl centorid variable at the specified position
            KMeans.centroids.set(position, new_centroid);
            this.counter++;
            context.write(new Text(new_centroid.toString()), new Text(Integer.toString(counter)));
          
        }

        /*
            This function returns the position of the centroid key in the global centroid variable in order to replace it with the new centroid
            @Param: Centroid point in Text format
            @return: poistion (integer value)

        */

        public static int findPosition(Text key)
        {
        	Point searchPoint= new Point(key.toString());
            float distance=0;
        	for (int i=0;i< UpdateJobRunner.centroids_old.size();i++)
        	{
        		distance= Point.distance(searchPoint,UpdateJobRunner.centroids_old.get(i));
                if (distance == 0.0)
                    return i;
        		
        	}
        	return 0;

        }



}
