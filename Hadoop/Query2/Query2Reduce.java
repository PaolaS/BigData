import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/* QUERY 2:
	weekly percentages of delays that are due to weather, throughout the entire data set
*/

public class Query2Reduce extends MapReduceBase implements Reducer<YearWeekWritable, ObjectWritable, YearWeekWritable, FloatWritable> {

	// Output: <YearWeek, percentageDelayed>
	@Override
	public void reduce(YearWeekWritable key, Iterator<ObjectWritable> values, OutputCollector<YearWeekWritable, FloatWritable> output, Reporter reporter)
			throws IOException {
		
		Float delayed =0f;
		Float total = 0f;
		Float percentageDelayed = 0.0f;
		
		while(values.hasNext()){
			
			Object value = values.next().get();
			// checks missing values
			if (!value.equals("NA"))
			{
				// checks delay
				if(Float.parseFloat(value.toString())>0){
					delayed = delayed + 1;
				}
			}
			
			total = total + 1;

		}
		
		percentageDelayed = (delayed/total);
		
		output.collect(key, new FloatWritable(percentageDelayed));
		
	}
	
}
