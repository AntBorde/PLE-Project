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
			x = lat / TileReduce.getLatTile();
			y = lng / (TileReduce.sizeTile / (TileReduce.METER_PER_DEG_EQUATEUR * Math.cos(((x * TileReduce.getLatTile()) * Math.PI) / 180)));
			return new LocKey((int) x, (int) y);
		} catch (Exception e) {
			return new LocKey(1000000, 1000000);
		}
	}
	
	public static LocKey getKey(Double latitude, double longitude) { // renvoi une clé en fonction de la latitude et
		// longitude qui correspond au "coordoné" de la tuile

		double x;
		double y;
		x = latitude / TileReduce.getLatTile();
		// System.out.println(" x = " + x);
		// System.out.println(((int)x * TileReduce.getLatTile()));
		y = longitude / (TileReduce.sizeTile / (TileReduce.METER_PER_DEG_EQUATEUR
				* Math.cos((((int) x * TileReduce.getLatTile()) * Math.PI) / 180)));
		// System.out.println(" y = "+y);
		return new LocKey((int) x, (int) y);

	}
	
	public void generateLocTileCenter(LocKey key) {
		double latTile = TileReduce.getLatTile();
		lat = ((double)key.getX()*latTile)+latTile/2;
		double lngTile = TileReduce.sizeTile/(TileReduce.METER_PER_DEG_EQUATEUR * Math.cos((lat * Math.PI) / 180));
		//System.out.println(lngTile);
		lng = ((double)key.getY() * lngTile) + lngTile/2;
		
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
		out.writeInt(h);
		
	}

}
