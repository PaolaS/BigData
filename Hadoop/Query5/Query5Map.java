import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/* QUERY 5:
 * ----------------------------------------------
*/


// Output: <year, cancellationCode>
public class Query5Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	private Text year = new Text();		//line[0]
	private Text cancCode = new Text();	//line[22]

	// Output <year, cancellationCode>
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		String[] line = value.toString().split(",");
		
		if (!(line[0].equals("Year"))){
			
			//We consider only those flights that were cancelled and have a cancellation code
			if(line[22].equals("A") || line[22].equals("B") || line[22].equals("C") || line[22].equals("D"))	{
				
				year.set(line[0]);
				cancCode.set(line[22]);
				output.collect(year, cancCode);
				
			}
		}
		
	}

}
