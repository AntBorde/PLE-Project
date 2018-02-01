package bigdata;

import java.io.Serializable;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class Const implements Serializable {
	
	
	private static final long serialVersionUID = 1L;
	
	
	/**
	 * constante de calcul
	 */
	public static final int NB_ZOOM = 10;
	public static final double SIZE = 100; //taille des tuiles de base en mètre
	public static final double FIRST_AGGREGATION_SIZE = 90; // taille de l'aggrégation 0 qui permet de corriger la courbure de la terre.
															// 90m étant environs la précision en latitude de la terre au format hgt.
	public static final int METER_PER_DEG_EQUATEUR = 111319; //wikipedia
	public static final int CANVAS_SIZE = 256*256;
	/**
	 * constant des table hbase
	 */
	public static final String NAMETABLE = "antCleImgFinal";
	public static final TableName TABLENAME = TableName.valueOf(NAMETABLE);
	public static final byte[] COLVALUE = Bytes.toBytes("value");
	public static final byte[] COLX = Bytes.toBytes("x");
	public static final byte[] COLY = Bytes.toBytes("y");
	
	
	
	/**
	 * retourne le nombre de degré de latitude pour une taille donnée (en mètre)
	 */
	
	public static double getLatTile(double size) {
		return size/METER_PER_DEG_EQUATEUR;
	}
	
	

}
