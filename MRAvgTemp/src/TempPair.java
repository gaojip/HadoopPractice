import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class TempPair implements Writable, WritableComparable<TempPair> {
	
	private IntWritable temp;
	private IntWritable count;
	
	public TempPair() {
		set(new IntWritable(0), new IntWritable(0));
	}
	
	public void set(int temp, int count) {
		this.temp.set(temp);
		this.count.set(count);
	}
	
	public void set(IntWritable temp, IntWritable count) {
		this.temp = temp;
		this.count = count;
	}

	@Override
	public int compareTo(TempPair o) {
		int compareVal = this.temp.compareTo(o.getTemp());
		if (compareVal != 0) {
			return compareVal;
		}
		return this.count.compareTo(o.getCount());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		temp.readFields(in);
		count.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		temp.write(out);
		count.write(out);
	}
	
	public static TempPair read(DataInput in) throws IOException {
		TempPair pair = new TempPair();
		pair.readFields(in);
		return pair;
	}
	
	public IntWritable getTemp() {
		return temp;
	}
	
	public IntWritable getCount() {
		return count;
	}

	@Override
	public String toString() {
		return "TempPair{temp=" + temp + ", count=" + count + "}";
	}

	@Override
	public int hashCode() {
		int result = temp.hashCode();
		result = 163 * result + count.hashCode();
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
		TempPair other = (TempPair) obj;
		if (count == null) {
			if (other.count != null)
				return false;
		} else if (!count.equals(other.count))
			return false;
		if (temp == null) {
			if (other.temp != null)
				return false;
		} else if (!temp.equals(other.temp))
			return false;
		return true;
	}

}
