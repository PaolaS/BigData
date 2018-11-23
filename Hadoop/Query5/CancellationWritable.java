import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

/*
 * Creates a Writable with the percentages of cancelled flights for every code
 */

public class CancellationWritable implements WritableComparable<CancellationWritable>{

	private FloatWritable percA;
	private FloatWritable percB;
	private FloatWritable percC;
	private FloatWritable percD;
	
	
	public CancellationWritable() {
		
		this.percA = new FloatWritable();
		this.percB = new FloatWritable();
		this.percC = new FloatWritable();
		this.percD = new FloatWritable();
	
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.percA.readFields(in);
		this.percB.readFields(in);
		this.percC.readFields(in);
		this.percD.readFields(in);

		
	}

	@Override
	public void write(DataOutput out) throws IOException {
	
		this.percA.write(out);
		this.percB.write(out);
		this.percC.write(out);
		this.percD.write(out);

	}

	

	public FloatWritable getPercA() {
		return percA;
	}


	public void setPercA(FloatWritable percA) {
		this.percA = percA;
	}


	public FloatWritable getPercB() {
		return percB;
	}


	public void setPercB(FloatWritable percB) {
		this.percB = percB;
	}


	public FloatWritable getPercC() {
		return percC;
	}


	public void setPercC(FloatWritable percC) {
		this.percC = percC;
	}


	public FloatWritable getPercD() {
		return percD;
	}


	public void setPercD(FloatWritable percD) {
		this.percD = percD;
	}


	public void set(FloatWritable percA, FloatWritable percB, FloatWritable percC, FloatWritable percD) {
		this.percA = percA;
		this.percB = percB;
		this.percC = percC;
		this.percD = percD;
	}

	@Override
	public int compareTo(CancellationWritable o) {
		return ComparisonChain.start().compare(percA, o.getPercA()).compare(percB, o.getPercB())
				.compare(percC,  o.getPercC()).compare(percD, o.getPercD()).result();
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((percA == null) ? 0 : percA.hashCode());
		result = prime * result + ((percB == null) ? 0 : percB.hashCode());
		result = prime * result + ((percC == null) ? 0 : percC.hashCode());
		result = prime * result + ((percD == null) ? 0 : percD.hashCode());
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
		CancellationWritable other = (CancellationWritable) obj;
		if (percA == null) {
			if (other.percA != null)
				return false;
		} else if (!percA.equals(other.percA))
			return false;
		if (percB == null) {
			if (other.percB != null)
				return false;
		} else if (!percB.equals(other.percB))
			return false;
		if (percC == null) {
			if (other.percC != null)
				return false;
		} else if (!percC.equals(other.percC))
			return false;
		if (percD == null) {
			if (other.percD != null)
				return false;
		} else if (!percD.equals(other.percD))
			return false;
		return true;
	}


	@Override
	public String toString() {
		return "CancellationWritable [percA=" + percA + ", percB=" + percB + ", percC=" + percC + ", percD=" + percD
				+ "]";
	}
	
	

	
	

}
