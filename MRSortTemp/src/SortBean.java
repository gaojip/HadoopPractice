import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class SortBean implements WritableComparable<SortBean> {
	
	public Text stationID;
	public IntWritable temp;

	public SortBean() {
		set(new Text(), new IntWritable());
	}
	
	public SortBean(Text stationID, IntWritable temp) {
		this.stationID = stationID;
		this.temp = temp;
	}
	
	public void set(String stationID, int temp) {
		this.stationID.set(stationID);
		this.temp.set(temp);
	}
	
	public void set(Text stationID, IntWritable temp) {
		this.stationID = stationID;
		this.temp = temp;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		stationID = new Text(dataInput.readUTF());
		temp = new IntWritable(dataInput.readInt());
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(stationID.toString());
		dataOutput.writeInt(temp.get());
	}

	@Override
	public int compareTo(SortBean o) {
		if (o == null) {
			throw new RuntimeException();
		}
		
		if (this.stationID.compareTo(o.getStationID()) == 0) {
			return -temp.compareTo(o.getTemp());
		}
		
		return this.stationID.compareTo(o.getStationID());
	}

	public Text getStationID() {
		return stationID;
	}

	public void setStationID(Text stationID) {
		this.stationID = stationID;
	}

	public IntWritable getTemp() {
		return temp;
	}

	public void setTemp(IntWritable temp) {
		this.temp = temp;
	}

	@Override
	public String toString() {
		return stationID + "	" + temp + "	";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((stationID == null) ? 0 : stationID.toString().hashCode());
		result = prime * result + ((temp == null) ? 0 : temp.get());
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
		SortBean other = (SortBean) obj;
		if (stationID == null) {
			if (other.stationID != null)
				return false;
		} else if (!stationID.equals(other.stationID))
			return false;
		if (temp == null) {
			if (other.temp != null)
				return false;
		} else if (!temp.equals(other.temp))
			return false;
		return true;
	}
}
