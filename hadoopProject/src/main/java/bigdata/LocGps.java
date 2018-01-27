package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class LocGps implements Writable {

	private double lat;
	private double lng;
	private int h;

	public LocGps() {
	}

	public LocGps(double lat, double lng, int h) {
		this.lat = lat;
		this.lng = lng;
		this.h = h;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLng() {
		return lng;
	}

	public void setLng(double lng) {
		this.lng = lng;
	}

	public int getH() {
		return h;
	}

	public void setH(int h) {
		this.h = h;
	}

	public LocKey getKey() {  //renvoi une clé en fonction de la latitude et 
												//longitude qui correspond au "coordoné" de la tuile

		try {
			double x;
			double y;
			double latitudeTile;
			x = lat / App.getLatTile();
			latitudeTile = x * App.getLatTile();
			y = lng / (App.sizeTile / (App.METER_PER_DEG_EQUATEUR * Math.cos((latitudeTile * Math.PI) / 180)));
			return new LocKey((int) x, (int) y);
		} catch (Exception e) {
			return new LocKey(1000000, 1000000);
		}
	}

	public String toString() {
		return (lat + "," + lng + "," + h);
	}

	public void readFields(DataInput in) throws IOException {
		lat = in.readDouble();
		lng = in.readDouble();
		h = in.readInt();
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(lat);
		out.writeDouble(lng);
		out.writeDouble(h);
		
	}

}
