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
	
	private ArrayList<LocGPS> locsGPS;
	
	
	public LocGPS() {
		locsGPS = new ArrayList<LocGPS>();
		
		
	}
	
	public LocGPS(double lat,double lng, int h) {
		this.lat=lat;
		this.lng=lng;
		this.h=h;
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
	
	public Tuple2<Integer,Integer> getKey() {
		
		try {
		double x;
		double y;
		double latitudeTile;
		x = lat/Main.getLatTile();
		latitudeTile = x*Main.getLatTile();
		y = lng/(Main.sizeTile/(Main.METER_PER_DEG_EQUATEUR*Math.cos((latitudeTile*Math.PI)/180)));
		return new Tuple2 <Integer,Integer> ((int)x,(int)y);
		}
		catch (Exception e) {return new Tuple2<Integer, Integer>(1000000, 1000000);}
		
	}

	@Override
	public Iterator<LocGPS> iterator() {
		Iterator<LocGPS> iLoc = locsGPS.iterator();
		return iLoc;
	}
	
	public String toString() {
		return ("latitude : "+ lat + " longitude : " + lng + " hauteur : " + h);
	}
	
	/*public LocGPS getLoc() {
		return locsGPS.get(index)
	}*/
	
	
}
