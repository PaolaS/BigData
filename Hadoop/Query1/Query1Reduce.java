import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *  QUERY 1:
 *  	the percentage of canceled flights per day, 
 *  	throughout the entire data set
 */

// Output: <Date, percentageCancelled>
public class Query1Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter)
			throws IOException {
		
		Float sum=0.0f;			// number of delayed flights
		Float total = 0.0f;		// total number of flights
		Float percentageCancelled = 0.0f;
		
		while(values.hasNext()){
			
			sum += values.next().get();
			total = total + 1;
		
		}
		
		percentageCancelled = sum/total;
		
		output.collect(key, new FloatWritable(percentageCancelled));
		
	}
	
}
