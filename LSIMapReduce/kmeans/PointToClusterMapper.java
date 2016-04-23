import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * You can modify this class as you see fit.  You may assume that the global
 * centroids have been correctly initialized.
 */
public class PointToClusterMapper extends Mapper<Text, Text, Text, Text>
{

	public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException
        {
		
            Point p = new Point(key.toString());    
            // compute the distance of the point from every centroid and emit the <centroid, point> pair
            float distance;
            float minDistance= Float.MAX_VALUE;
            Point centroid=null;
            for(int i=0;i< KMeans.centroids.size();i++)
            	{
            	distance= Point.distance(p,KMeans.centroids.get(i));
            	if (distance < minDistance)
            		{
            			centroid= KMeans.centroids.get(i);
            			minDistance=distance;
            		}
            	}
            context.write(new Text(centroid.toString()), new Text(p.toString()));
        }


}
