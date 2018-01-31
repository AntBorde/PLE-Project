package bigdata;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;
import scala.Tuple3;



public class Main implements Serializable {
	

	private static final long serialVersionUID = 1L;
	
	

	public static void main(String[] args) throws IOException {
		
		//String input = args[0];
		//int nbZoom = Integer.parseInt(args[1]);
		int nbZoom = 3;
		double size = 1000;
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//InitTable(nbZoom);
		
		JavaRDD<LocGPS> firstRDD = context.textFile("hdfs://beetlejuice:9000/user/antborde/cut.txt").map((x)->
		{
			short nbReduce = 0;
			String[] parts = x.split(",");
			return new LocGPS(Double.parseDouble(parts[0]),LocGPS.correctionLongitude(Double.parseDouble(parts[1])),Integer.parseInt(parts[2]),nbReduce);
		});
		
		//firstWriteBD(context, firstRDD);
		
		
		JavaPairRDD<Tuple2<Integer,Integer>,Iterable<LocGPS>> locRDD = firstRDD.mapToPair((x)->
		{
			Tuple2<Integer,Integer> key = x.getKey(x.getLat(),x.getLng(),size);
			Tuple2<Tuple2<Integer,Integer>,LocGPS> val = new Tuple2<Tuple2<Integer,Integer>,LocGPS>(key,x);
			return val;
		}).groupByKey();
		
		
		
		for(int i=1 ; i<=nbZoom ; i++) {
		
		JavaPairRDD<Tuple2<Integer,Integer>,LocGPS> locRDDAgg = locRDD.mapValues((x)->{
			int moyH = 0;
			int c =0;
			for(LocGPS loc : x) {
				moyH += loc.getH();
				c++;
			}
			moyH = moyH/c;
			Iterator<LocGPS> it = x.iterator();
			if(it.hasNext()) {
				LocGPS loc = it.next();
				return new LocGPS(loc.getLat(),loc.getLng(),moyH,loc.getNbReduce());
			}
			return new LocGPS();
		});
		
		//writeBD(context,locRDDAgg,size);
		
		locRDD = locRDDAgg.mapToPair((x)->
		{
			int keyX = x._1._1;
			int keyY = x._1._2;
			short nbReduce = x._2.getNbReduce();
			int h = x._2.getH();
			LocGPS loc = new LocGPS(keyX,keyY,h,size,nbReduce);
			Tuple2<Integer,Integer> key = loc.getKey(loc.getLat(),loc.getLng(),size);
			Tuple2<Tuple2<Integer,Integer>,LocGPS> val = new Tuple2<Tuple2<Integer,Integer>,LocGPS>(key,loc);
			return val;
		}).groupByKey();
		
	}

		context.close();
		
	}
	
	/*private static void InitTable(int nbZoom) throws IOException {
		Connection connection = ConnectionFactory.createConnection();
		Admin admin = connection.getAdmin();
		if (admin.tableExists(Const.TABLENAME)) {
			admin.disableTable(Const.TABLENAME);
			admin.deleteTable(Const.TABLENAME);
		}
		
		HTableDescriptor tableDesc = new HTableDescriptor(Const.TABLENAME);
		tableDesc.addFamily(new HColumnDescriptor("zoom0"));
		for(int i=1 ; i<=nbZoom ; i++) {
			tableDesc.addFamily(new HColumnDescriptor("zoom"+i));
		}
		admin.createTable(tableDesc);
		connection.close();
	}
	
	
	private static void writeBD(JavaSparkContext context, JavaPairRDD<Tuple2<Integer, Integer>, LocGPS> locs ,double size) {
		locs.foreachPartition(it -> {
			Connection conn = ConnectionFactory.createConnection();
			Table table = conn.getTable(Const.TABLENAME);
			while (it.hasNext()) {
				Tuple2<Tuple2<Integer, Integer>, LocGPS> loc = it.next();
				
				int keyX = loc._1._1;
				int keyY = loc._1._2;
				LocGPS p = loc._2;
				p.generateLocTileCenter(keyX, keyY, size);
				
				byte[] fam = Bytes.toBytes("zoom"+(p.getNbReduce()+1));
				
				String id ="z"+(p.getNbReduce()+1)+","+keyX/Const.CANVAS_SIZE+","+keyY/Const.CANVAS_SIZE;
				Put row = new Put(Bytes.toBytes(id.toString()));
				row.addColumn(fam, Const.COL_LAT, Bytes.toBytes(new Double(p.getLat()).toString()));
				row.addColumn(fam, Const.COL_LNG, Bytes.toBytes(new Double(p.getLng()).toString()));
				row.addColumn(fam, Const.COLVALUE, Bytes.toBytes(new Integer(p.getH()).toString()));
				row.addColumn(fam, Const.COLX, Bytes.toBytes(new Integer(keyX).toString()));
				row.addColumn(fam, Const.COLY, Bytes.toBytes(new Integer(keyY).toString()));
				table.put(row);
			}
			conn.close();
		});
	}
	
	private static void firstWriteBD(JavaSparkContext context, JavaRDD<LocGPS> locs) {
		locs.foreachPartition(it -> {
			Connection conn = ConnectionFactory.createConnection();
			Table table = conn.getTable(Const.TABLENAME);
			byte[] fam = Bytes.toBytes("zoom0");
			while (it.hasNext()) {
				LocGPS loc = it.next();
				UUID id = UUID.randomUUID();
				Put row = new Put(Bytes.toBytes(id.toString()));
				row.addColumn(fam, Const.COLX, Bytes.toBytes(new Double(loc.getLat()).toString()));
				row.addColumn(fam, Const.COLY, Bytes.toBytes(new Double(loc.getLng()).toString()));
				row.addColumn(fam, Const.COLVALUE, Bytes.toBytes(new Integer(loc.getH()).toString()));
				table.put(row);
			}
			conn.close();
		});
	}*/
	
	


}

