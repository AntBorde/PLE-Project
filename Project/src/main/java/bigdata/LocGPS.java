package bigdata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import scala.Tuple2;

public class LocGPS implements Serializable, Iterable<LocGPS> {

	private static final long serialVersionUID = 1L;

	private double lat;
	private double lng;
	private int h;
	private short nbReduce;

	private ArrayList<LocGPS> locsGPS;

	public LocGPS() {
		locsGPS = new ArrayList<LocGPS>();

	}
	
	public LocGPS(double lat, double lng, int h, short nbReduce) {

		this.lat = lat;
		this.lng = lng;
		this.h = h;
		this.nbReduce = nbReduce;
		locsGPS = new ArrayList<LocGPS>();
	}

	public LocGPS(int x, int y, int h, short nbReduce) {

		this.h = h;
		this.nbReduce = nbReduce;
		generateLocTileCenter(x, y, Const.SIZE);
		this.nbReduce++;
		locsGPS = new ArrayList<LocGPS>();
	}

	public short getNbReduce() {
		return nbReduce;
	}

	public void setNbReduce(short nbReduce) {
		this.nbReduce = nbReduce;
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

	public Iterator<LocGPS> iterator() {
		Iterator<LocGPS> iLoc = locsGPS.iterator();
		return iLoc;
	}
	
	public String toString() {
		return ("latitude : " + lat + " longitude : " + lng + " hauteur : " + h);
	}

	/**
	 * Attribut au point la latitute et longitude correspondant au centre de ça tuile d'aggrégation.
	 */
	public void generateLocTileCenter(int x, int y, double size) {
		size = size * Math.pow(2, nbReduce);
		double latTile = Const.getLatTile(size);
		lat = ((double) x * latTile) + latTile / 2;
		double lngTile = size / (Const.METER_PER_DEG_EQUATEUR * Math.cos((lat - 90 * Math.PI) / 180));
		lng = (y * lngTile) + lngTile / 2;
	}

	
	public Tuple2<Integer, Integer> getKey(double size) {
		size = size * Math.pow(2, nbReduce);
		double x;
		double y;
		x = lat / Const.getLatTile(size);
		y = lng / (size / (Const.METER_PER_DEG_EQUATEUR * Math.cos((lat - 90 * Math.PI) / 180)));
		return new Tuple2<Integer, Integer>((int) x, (int) y);

	}

	
	public static double correctionLongitude(double y) {
		double x = Math.floor(y);
		return (x + ((y - x) * 1000));
	}

}
