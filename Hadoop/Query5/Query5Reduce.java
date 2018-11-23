import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/* QUERY 5: 
 * the yearly percentage of flights cancelled for each reason (A = carrier, B = weather, C = NAS, D = security)
*/

// Output: <Year, percentagesCancelled>
public class Query5Reduce extends MapReduceBase implements Reducer<Text, Text, Text, CancellationWritable> {

	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, CancellationWritable> output, Reporter reporter)
			throws IOException {
	
		CancellationWritable percentages = new CancellationWritable();
		Float cancA = 0.0f;
		Float cancB = 0.0f;
		Float cancC = 0.0f;
		Float cancD = 0.0f;
		Float total = 0.0f; 
		
		while(values.hasNext()){
			
			String value = values.next().toString();
			
			// Count every flight cancelled for each cancellation code
			switch (value) {
			case "A":
				cancA = cancA + 1;
				break;
			case "B":
				cancB = cancB + 1;
				break;
			case "C":
				cancC = cancC + 1; 
				break;
			case "D":
				cancD = cancD + 1;
				break;
			default:
				break;
			}
			
			// Count the total amount of flights
			total = total + 1;
			
		}
		
		percentages.setPercA(new FloatWritable(cancA/total));
		percentages.setPercB(new FloatWritable(cancB/total));
		percentages.setPercC(new FloatWritable(cancC/total));
		percentages.setPercD(new FloatWritable(cancD/total));
				
		output.collect(key, percentages);

	}
	
}
