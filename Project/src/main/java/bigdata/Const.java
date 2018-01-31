package bigdata;

import java.io.Serializable;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class Const implements Serializable {
	
	
	private static final long serialVersionUID = 1L;
	
	public static double sizeTile = 1000;
	public static final int METER_PER_DEG_EQUATEUR = 111319; //wikipedia 139908092
	public static final String NAMETABLE = "antCleImgtest";
	public static final TableName TABLENAME = TableName.valueOf(NAMETABLE);
	public static final byte[] COL_LAT = Bytes.toBytes("lat");
	public static final byte[] COL_LNG = Bytes.toBytes("lng");
	public static final byte[] COLVALUE = Bytes.toBytes("value");
	public static final byte[] COLX = Bytes.toBytes("x");
	public static final byte[] COLY = Bytes.toBytes("y");
	public static final int CANVAS_SIZE = 256*256;
	//public static final double DEGRE_PER_POINT_HGT_FORMAT =  ((double)1)/1201;
	
	
	public static double getSize() {
		return sizeTile;
	}
	
	public static void setSize(int size) {
		sizeTile=size;
	}
	
	public static double getLatTile() {
		return sizeTile/METER_PER_DEG_EQUATEUR;
	}
	
	public static double getLatTile(double size) {
		System.out.println(size/METER_PER_DEG_EQUATEUR);
		return size/METER_PER_DEG_EQUATEUR;
		
	}
	
	

}
