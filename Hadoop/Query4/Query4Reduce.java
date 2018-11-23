import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/* QUERY 4: 
	a weekly "penalty" score for each airport that depends on both 
	its incoming and outgoing flights. The score adds 0.5 for each 
	incoming flight that is more than 15 minutes late, and 1 for 
	each outgoing flight that is more than 15 minutes late
*/

// Output: <Year-week-airport, penalty>
public class Query4Reduce extends MapReduceBase implements Reducer<YearWeekAirportWritable, DoubleWritable, YearWeekAirportWritable, DoubleWritable> {

	@Override
	public void reduce(YearWeekAirportWritable key, Iterator<DoubleWritable> values, OutputCollector<YearWeekAirportWritable, DoubleWritable> output, Reporter reporter)
			throws IOException {
	
		double penalty = 0;
		
		while(values.hasNext()){
			
			double value = values.next().get();
			penalty = penalty + value;
			
		}
		
		output.collect(key, new DoubleWritable(penalty));

	}
	
}
