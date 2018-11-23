import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

/*
 * Creates a <Year, Week, Airport> Writable 
 */

public class YearWeekAirportWritable implements WritableComparable<YearWeekAirportWritable>{

	private Text year;
	private Text week;
	private Text airport;
	
	public YearWeekAirportWritable() {
		
		this.year = new Text();
		this.week = new Text();
		this.airport = new Text();
	
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.year.readFields(in);
		this.week.readFields(in);
		this.airport.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
	
		this.year.write(out);
		this.week.write(out);
		this.airport.write(out);
		
	}

	
	public Text getYear() {
		return year;
	}

	public void setYear(Text year) {
		this.year = year;
	}

	public Text getWeek() {
		return week;
	}

	public void setWeek(Text week) {
		this.week = week;
	}

	public Text getAirport() {
		return airport;
	}

	public void setAirport(Text airport) {
		this.airport = airport;
	}

	public void set(Text year, Text week, Text airport) {
		this.year = year;
		this.week = week;
		this.airport = airport;
	}

	@Override
	public int compareTo(YearWeekAirportWritable o) {
		return ComparisonChain.start().compare(year, o.getYear()).compare(week, o.getWeek()).compare(airport,  o.getAirport()).result();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((airport == null) ? 0 : airport.hashCode());
		result = prime * result + ((week == null) ? 0 : week.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		YearWeekAirportWritable other = (YearWeekAirportWritable) obj;
		if (airport == null) {
			if (other.airport != null)
				return false;
		} else if (!airport.equals(other.airport))
			return false;
		if (week == null) {
			if (other.week != null)
				return false;
		} else if (!week.equals(other.week))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	@Override
	public String toString() {
		if (week.getLength()==1){
			return year + "-0" + week + "-" + airport ;
		}
		else{
			return year + "-" + week + "-" + airport ;
		}
	}

	
	

}
