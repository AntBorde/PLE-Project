package bigdata;

import java.io.Serializable;
import java.nio.ByteBuffer;
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
import org.apache.spark.sql.execution.columnar.BYTE;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;
import scala.Tuple3;

public class App2 implements Serializable {

	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws IOException {
		// String input = args[0];
		// int nbZoom = Integer.parseInt(args[1]);
		int nbZoom = 8;
		double size = 200;
		
		HbaseConnexion.InitTable(nbZoom);

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaPairRDD<Tuple2<Integer,Integer>,LocGPS> firstRDD = context.textFile("hdfs://beetlejuice:9000/raw_data/dem3_lat_lng.txt").mapToPair((x) -> {
			short nbReduce = 0;
			String[] parts = x.split(",");
			LocGPS loc= new LocGPS(Double.parseDouble(parts[0])+90, LocGPS.correctionLongitude(Double.parseDouble(parts[1])+180),
					Integer.parseInt(parts[2]), nbReduce);
			Tuple2<Integer,Integer> key = loc.getFirstKey();
			Tuple2<Tuple2<Integer,Integer>,LocGPS> val = new Tuple2<Tuple2<Integer,Integer>,LocGPS>(key,loc);
			return val;
		}).cache();
		
		
		
		for (int nbReduction = 0; nbReduction < nbZoom; nbReduction++) {
			
		
		JavaPairRDD<Tuple2<Integer,Integer>,Iterable<LocGPS>> tmpCanvas = firstRDD.mapToPair((x)->{
			Tuple2<Integer,Integer> key = new Tuple2<Integer,Integer>(x._1._1/256,x._1._2/256);
			Tuple2<Tuple2<Integer,Integer>,LocGPS> val = new Tuple2<Tuple2<Integer,Integer>,LocGPS>(key,x._2);
			return val;
		}).groupByKey();
		
				
		JavaPairRDD<Tuple2<Integer,Integer>,Canvas> canvas = tmpCanvas.mapValues((x)->{
			short[] values = new short[Const.CANVAS_SIZE];
			Tuple2<Integer,Integer> key;
			
			int i;
			for(LocGPS loc : x) {
				if(loc.getNbReduce()==0) {
					key = loc.getFirstKey();
				}
				else
					key = loc.getKey(size);
				i = ((key._1%256)*256)+(key._2%256);
				values[i]=(short)loc.getH();
			}
			ByteBuffer byteBuf = ByteBuffer.allocate(Const.CANVAS_SIZE*2);
			for ( int j = 0; j < Const.CANVAS_SIZE; j++) {
				byteBuf.putShort(values[j]);
			}
			byte[] image = byteBuf.array();
			return new Canvas((short)0,image);
		});
		
		HbaseConnexion.writeBD(context,canvas);
		
		JavaPairRDD<Tuple2<Integer,Integer>,Iterable<LocGPS>> locRDD = firstRDD.mapToPair((x)->
		{
			LocGPS loc = x._2;
			Tuple2<Integer,Integer> key = loc.getKey(size);
			Tuple2<Tuple2<Integer,Integer>,LocGPS> val = new Tuple2<Tuple2<Integer,Integer>,LocGPS>(key,x._2);
			return val;
		}).groupByKey();
		
		
		firstRDD = locRDD.mapValues((x)->{
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
				Tuple2<Integer,Integer> key = loc.getKey(size);
				return new LocGPS(key._1,key._2,moyH,size,loc.getNbReduce());
			}
			return new LocGPS(1,1,-1,(short)0);
		}).cache();
		
		
		}
		
		
	
		context.close();

		
	}
	
	
	

	
	
}
