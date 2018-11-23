import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
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

public class Query3Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DelaysWritable> {
	
	private DelaysWritable delaysWritable = new DelaysWritable();
	private IntWritable distanceGroup;
	private FloatWritable departureDelay;	//line[15]
	private FloatWritable arrivalDelay;		//line[14]
	
	
	// Output <distanceGroup, delaysWritable>
	@Override
	public void map(LongWritable key, Text value, OutputCollector<IntWritable, DelaysWritable> output, Reporter reporter) throws IOException {

		String[] line = value.toString().split(",");
		
		if (!(line[0].equals("Year") || line[14].equals("NA") || line[15].equals("NA") || line[18].equals("NA"))){
			
			// crops the flights with distance > 2600 miles
			if(Integer.parseInt(line[18]) < 2600){
				
				int distance = Integer.parseInt(line[18]);
				Double doubleObj = Math.floor(distance/200)+1;
				distanceGroup = new IntWritable(doubleObj.intValue());
				
				
				// if the departure delay is negative, both the departure delay
				// and the arrival delay are set to 1
				// (a negative value of this variable is not considered a real "delay") 
				if(Integer.parseInt(line[15]) <0){
					
					departureDelay = new FloatWritable(Float.parseFloat(("1")));
					arrivalDelay = new FloatWritable(Float.parseFloat(("1")));
				
				}
				
				else{
					
					departureDelay = new FloatWritable(Float.parseFloat((line[15])));
					arrivalDelay = new FloatWritable(Float.parseFloat(line[14]));
					
				}
				
				delaysWritable.set(departureDelay, arrivalDelay);
				
				output.collect(distanceGroup, delaysWritable);
				
			}
		}
		
	}

}
