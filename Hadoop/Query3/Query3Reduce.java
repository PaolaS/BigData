import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/* QUERY 3: 
	the percentage of flights belonging to a given "distance group" that were able 
	to halve their departure delays by the time they arrived at their destinations. 
	Distance groups assort flights by their total distance in miles. Flights with 
	distances that are less than 200 miles belong in group 1, flights with distances 
	that are between 200 and 399 miles belong in group 2, flights with distances that 
	are between 400 and 599 miles belong in group 3, and so on. The last group contains 
	flights whose distances are between 2400 and 2599 miles.
*/

// Output: <Distance group, percentageHalvedDelays>
public class Query3Reduce extends MapReduceBase implements Reducer<IntWritable, DelaysWritable, IntWritable, FloatWritable> {

	@Override
	public void reduce(IntWritable key, Iterator<DelaysWritable> values, OutputCollector<IntWritable, FloatWritable> output, Reporter reporter)
			throws IOException {
		
		Float halved =0f;
		Float total = 0f;
		Float percentageHalved = 0.0f;
		Float departureDelay;
		Float arrivalDelay;
		
		while(values.hasNext()){
			
			DelaysWritable value = values.next().get();
			departureDelay = value.getDepartureDelay().get();
			arrivalDelay = value.getArrivalDelay().get();
			
			// counts total amount of flights that managed to halve the delay per distance group
			if (departureDelay >= arrivalDelay*2)
			{
				halved = halved + 1;
			}
			
			// counts total amount of flights per distance group
			total = total + 1;
			
		}
		
		percentageHalved = halved/total;
		
		output.collect(key, new FloatWritable(percentageHalved));

	}
	
}
