package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class LocKey implements WritableComparable<LocKey> {
	
	private int x;
	private int y;
	
	
	public LocKey() {
		
	}
	
	public LocKey(int x1, int y1) {
		x=x1;
		y=y1;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(x);
		out.writeInt(y);
	}
	

	public void readFields(DataInput in) throws IOException {
		x = in.readInt();
		y = in.readInt();
	}
	
	
	public int compareTo(LocKey arg0) {
		if(x-arg0.getX() ==0 || y - arg0.getY()==0)
			return 0;
		else 
			return -1;
			
	}
	
	public int getX (){
		return x;
	}
	
	public int getY (){
		return y;
	}
	
	public void setX(int x1) {
		x=x1;
	}
	
	public void setY(int y1) {
		y=y1;
	}


}
