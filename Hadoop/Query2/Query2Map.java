import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/* QUERY 2:
	weekly percentages of delays that are due to weather, throughout the entire data set
*/

public class Query2Map extends MapReduceBase implements Mapper<LongWritable, Text, YearWeekWritable, ObjectWritable> {

	
	private ObjectWritable weatherDelay = new ObjectWritable(); //line[25]
	private Text year;											//line[0]
	private Text week;
	
	// Output: <Year-week, weatherDelay>
	@Override
	public void map(LongWritable key, Text value, OutputCollector<YearWeekWritable, ObjectWritable> output, Reporter reporter) throws IOException {

		String[] line = value.toString().split(",");
		
		if (!(line[0].equals("Year") )){
			
			// Builds date
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance(Locale.ITALIAN); 
			cal.setFirstDayOfWeek(Calendar.MONDAY);

			String strDate = line[0]+"-";
			String month = line[1];
			
			int weekNr = 0;
			int yearNr = 0;
			
			if(month.length()==1){
				strDate = strDate.concat("0"+month+"-");
			}
			else{
				strDate = strDate.concat(month+"-");
			}
			if(line[2].length()==1){
				strDate = strDate.concat("0"+line[2]);
			}
			else{
				strDate = strDate.concat(line[2]);
			}
			
			try {
				Date dateDate = sdf.parse(strDate);
				
				cal.setTime(dateDate);
				weekNr = cal.get(Calendar.WEEK_OF_YEAR);
				yearNr = Integer.parseInt(line[0]);
				week = new Text(String.valueOf(weekNr));
				
				// This is used to properly set the year with respect to the week
				if (Integer.parseInt(month) == 1 && weekNr > 50){
					year = new Text(String.valueOf((yearNr - 1)));
				}
				else if (Integer.parseInt(month) == 12 && weekNr == 1) {
					year = new Text(String.valueOf((yearNr + 1)));
				}
				else {
					year = new Text(line[0]);
				}
				
				
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			YearWeekWritable ywwKey = new YearWeekWritable();
			
			ywwKey.set(year, week);
			weatherDelay.set(line[25]);
			
			output.collect(ywwKey, weatherDelay);
		
		}
			
	}

}
