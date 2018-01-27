package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Elevation implements Writable {
	
	private long height;
	private int poids;
	
	
	public Elevation() {
		
	}
	
	public Elevation (long h) {
		height = h;
		poids = 1;
	}
	
	public Elevation(long h ,int p) {
		height= h;
		poids = p;
	}
	
	
	public long getHeight() {
		return height;
	}
	public void setHeight(int height) {
		this.height = height;
	}
	public int getPoids() {
		return poids;
	}
	public void setPoids(int poids) {
		this.poids = poids;
	}


	public void write(DataOutput out) throws IOException {
		out.writeLong(height);
		out.writeInt(poids);
	}


	public void readFields(DataInput in) throws IOException {
		height = in.readLong();
		poids = in.readInt();
	}

}
