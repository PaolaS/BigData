import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/* QUERY 4: 
	a weekly "penalty" score for each airport that depends on both 
	its incoming and outgoing flights. The score adds 0.5 for each 
	incoming flight that is more than 15 minutes late, and 1 for 
	each outgoing flight that is more than 15 minutes late
*/


// Output: <Year-Week-Airport, penalty>
public class Query4Map extends MapReduceBase implements Mapper<LongWritable, Text, YearWeekAirportWritable, DoubleWritable> {

	private Text year;		//line[0]
	private Text week;
	private Text airport;
	private YearWeekAirportWritable writable = new YearWeekAirportWritable();
	private DoubleWritable penalty = new DoubleWritable();
	private Double depPenalty = 1.0;
	private Double arrPenalty = 0.5;
	
	
	// departureDelay	line[15]
	// arrivalDelay		line[14]
	// departureAirport	line[16]
	// arrivalAirport	line[17]
	
	// Output <distanceGroup, delaysWritable>
	@Override
	public void map(LongWritable key, Text value, OutputCollector<YearWeekAirportWritable, DoubleWritable> output, Reporter reporter) throws IOException {

		String[] line = value.toString().split(",");
		
		if (!(line[0].equals("Year"))){
				
			// Builds date
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance(Locale.ITALIAN);
			cal.setFirstDayOfWeek(Calendar.MONDAY);

			String strDate = line[0]+"-";
			String month = line[1];
			int weekNr = 0;
			int yearNr = 0;
			
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
			
			try {
				Date dateDate = sdf.parse(strDate);
				cal.setTime(dateDate);
				week = new Text(String.valueOf(cal.get(Calendar.WEEK_OF_YEAR)));
				weekNr = cal.get(Calendar.WEEK_OF_YEAR);
				yearNr = Integer.parseInt(line[0]);
				week = new Text(String.valueOf(weekNr));
				
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
			
		//Every rows is tested on both the if below
			
			// checks if it has departure delays 
			if (!(line[15].equals("NA")) && !(line[16].equals("NA")) && Integer.parseInt(line[15]) > 15){
				
				// departure airport
				airport = new Text(line[16]);
				
				writable.set(year, week, airport);
				penalty.set(depPenalty);
				
				output.collect(writable, penalty);
				
			}
			
			// checks if it has arrival delays 
			if (!(line[14].equals("NA")) && !(line[17].equals("NA")) && Integer.parseInt(line[14]) > 15){
				
				// arrival airport
				airport = new Text(line[17]);
				
				writable.set(year, week, airport);
				penalty.set(arrPenalty);
				
				output.collect(writable, penalty);
				
			}
				
		}
		
	}

}
