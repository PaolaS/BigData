import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;
/**
 *	Creates a <departureDelay, arrivalDelay> Writable
 */
public class DelaysWritable implements WritableComparable<DelaysWritable>{

	private FloatWritable departureDelay;
	private FloatWritable arrivalDelay;
	
	public DelaysWritable(FloatWritable departureDelay, FloatWritable arrivalDelay) {
		super();
		this.departureDelay = departureDelay;
		this.arrivalDelay = arrivalDelay;
	}

	public DelaysWritable() {
		this.departureDelay = new FloatWritable();
		this.arrivalDelay = new FloatWritable();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.departureDelay.readFields(in);
		this.arrivalDelay.readFields(in);	
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.departureDelay.write(out);
		this.arrivalDelay.write(out);
	}

	public void set(FloatWritable departureDelay, FloatWritable arrivalDelay) {
		this.departureDelay = departureDelay;
		this.arrivalDelay = arrivalDelay;
	}
	
	public DelaysWritable get(){
		return this;
	}
	
	public FloatWritable getDepartureDelay() {	
		return departureDelay;
	}

	public FloatWritable getArrivalDelay() {
		return arrivalDelay;
	}

	@Override
	public int compareTo(DelaysWritable o) {
		return ComparisonChain.start().compare(departureDelay, o.departureDelay).compare(arrivalDelay, o.arrivalDelay).result();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((arrivalDelay == null) ? 0 : arrivalDelay.hashCode());
		result = prime * result + ((departureDelay == null) ? 0 : departureDelay.hashCode());
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
		DelaysWritable other = (DelaysWritable) obj;
		if (arrivalDelay == null) {
			if (other.arrivalDelay != null)
				return false;
		} else if (!arrivalDelay.equals(other.arrivalDelay))
			return false;
		if (departureDelay == null) {
			if (other.departureDelay != null)
				return false;
		} else if (!departureDelay.equals(other.departureDelay))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "DelaysWritable [departureDelay=" + departureDelay + ", arrivalDelay=" + arrivalDelay + "]";
	}

	

}
