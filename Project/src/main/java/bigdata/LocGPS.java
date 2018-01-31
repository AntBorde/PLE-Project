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
	private  short nbReduce;

	private ArrayList<LocGPS> locsGPS;

	public LocGPS() {
		locsGPS = new ArrayList<LocGPS>();

	}
	
	public LocGPS(int h,short nbReduce) {
		this.h=h;
		this.nbReduce=nbReduce;
		locsGPS = new ArrayList<LocGPS>();

	}

	public short getNbReduce() {
		return nbReduce;
	}

	public void setNbReduce(short nbReduce) {
		this.nbReduce = nbReduce;
	}

	public LocGPS(double lat, double lng, int h,short nbReduce) {

		this.lat = lat;
		this.lng = lng;
		this.h = h;
		this.nbReduce = nbReduce;
		locsGPS = new ArrayList<LocGPS>();
	}
	
	public LocGPS(int x, int y, int h,double size,short nbReduce) {

		this.h = h;
		this.nbReduce = nbReduce;
		generateLocTileCenter(x, y,size);
		this.nbReduce++;
		locsGPS = new ArrayList<LocGPS>();
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

	public Tuple2<Integer, Integer> getKey(double size) {
		size =size*Math.pow(2,nbReduce);
		double x;
		double y;
		x = lat / Const.getLatTile(size);
		y = lng / (size / (Const.METER_PER_DEG_EQUATEUR * Math.cos((lat-90 * Math.PI) / 180)));
		return new Tuple2<Integer, Integer>((int) x, (int) y);

	}

	public void generateLocTileCenter(int x, int y , double size) {
		size =size*Math.pow(2,nbReduce);
		double latTile = Const.getLatTile(size);
		lat = ((double) x * latTile) + latTile / 2;
		double lngTile = size / (Const.METER_PER_DEG_EQUATEUR * Math.cos((lat-90 * Math.PI) / 180));
		lng = (y * lngTile) + lngTile / 2;

	}

	@Override
	public Iterator<LocGPS> iterator() {
		Iterator<LocGPS> iLoc = locsGPS.iterator();
		return iLoc;
	}

	public String toString() {
		return ("latitude : " + lat + " longitude : " + lng + " hauteur : " + h);
	}

	public Tuple2<Integer, Integer> getKey(double l, double ln,double size) {
		size = size*Math.pow(2,nbReduce);
		double x;
		double y;
		x = l / Const.getLatTile(size);
		y = ln / (size / (Const.METER_PER_DEG_EQUATEUR * Math.cos(((x * Const.getLatTile(size)) * Math.PI) / 180)));
		return new Tuple2<Integer, Integer>((int) x, (int) y);

	}
	
	public Tuple2<Integer, Integer> getFirstKey (){
		double x = lat/(((double)1)/1201);
		double y = lng/(((double)1)/1201);
		return new Tuple2<Integer,Integer>((int)(x),(int)(y));
	}
	
	/*public Tuple2<Integer, Integer> getFirstCanvasKey (){
		double x = lat/Const.DEGRE_PER_POINT_HGT_FORMAT;
		double y = lng/Const.DEGRE_PER_POINT_HGT_FORMAT;
		return new Tuple2<Integer,Integer>((int)(x/256),(int)(y/256));
	}*/

	/*public static double generateLatTileCenter(int x) {
		double latTile = Const.getLatTile();
		return ((double) x * latTile) + latTile / 2;
	}

	public static double generateLngTileCenter(int y, Double latitude) {
		double lngTile = Const.sizeTile / (Const.METER_PER_DEG_EQUATEUR * Math.cos((latitude * Math.PI) / 180));
		return (y * lngTile) + lngTile / 2;

	}*/

	public static double correctionLongitude(double y) {
		double x = Math.floor(y);
		return (x + ((y - x) * 1000));
	}
	
}
