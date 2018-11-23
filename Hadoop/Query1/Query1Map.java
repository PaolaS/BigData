import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Query1Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {

/**
 *  QUERY 1:
 *  	the percentage of canceled flights per day, 
 *  	throughout the entire data set
 */
	
	private Text date;
	private FloatWritable cancelled = new FloatWritable();	//line[21]
	
	// Output: <Date, cancelled>
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {

		String[] line = value.toString().split(",");
		
		if (!(line[0].equals("Year"))){
			
			String strDate = line[0]+"-";
			
			// builds the date Text properly
			if(line[1].length()==1){
				strDate = strDate.concat("0"+line[1]+"-");
			}
			else{
				strDate = strDate.concat(line[1]+"-");
			}
			if(line[2].length()==1){
				strDate = strDate.concat("0"+line[2]);
			}
			else{
				strDate = strDate.concat(line[2]);
			}
			
			date = new Text(strDate);
			
			//during the data exploration, we checked that there weren't missing values
			cancelled.set(Float.parseFloat(line[21]));
			
			output.collect(date, cancelled);
			
		}
		
	}

}
