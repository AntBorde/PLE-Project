package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;



public class Main {
	
	public static double sizeTile = 0;
	public static final int METER_PER_DEG_EQUATEUR = 111319; //wikipedia
	public static double latTile = 0;
	

	

	public static void main(String[] args) throws IOException {
		
		/*SparkSession spark = SparkSession.builder()
		.appName("Test")
	    .config("spark.some.config.option", "test spark")
	    .getOrCreate();
		
		
		setSize(1000);
		
		JavaRDD<String> initTextRDD = spark.sparkContext().textFile("hdfs://beetlejuice:9000/user/antborde/cut.txt",1).toJavaRDD();
		JavaRDD<LocGPS> tmpRDD = initTextRDD.flatMap((x)->
				{
					List<LocGPS> res = new ArrayList<LocGPS>();
					String[] parts = x.split(",");
					
					LocGPS loc = new LocGPS(Double.parseDouble(parts[0]),Double.parseDouble(parts[1]),Integer.parseInt(parts[2]));
					if(loc.getLat()<-90 && loc.getLng()>90 && loc.getLng()<-180 && loc.getLng()>180 && loc.getH()<0 && loc.getH()>9000) {
						
					}
					else {
						res.add(loc);
					}
					return res.iterator();
					
				});
		
		JavaPairRDD<Tuple2<Integer,Integer>,Iterable <LocGPS>> locRDD = tmpRDD.groupBy((x)->
				{
					System.out.println(x.getKey());
					return x.getKey();
					
				});
		
		System.out.println("||||||||||||||||||||||||||||--------------------------->>>>>>>>>>>>>>>>" + locRDD.first());*/
		
		setSize(1000);
		LocGPS test = new LocGPS(42.152,45.154,1000);
		Tuple2<Integer, Integer> tuple = test.getKey();
		System.out.println(test.toString());
		System.out.println(tuple.toString());
		
		System.out.println("---------------");
		int x = tuple._1;
		int y = tuple._2;
		
		System.out.println((x*Main.getLatTile()));
		System.out.println((y*(Main.sizeTile / (Main.METER_PER_DEG_EQUATEUR * Math.cos(((x*Main.getLatTile()) * Math.PI) / 180)))));
		
		System.out.println("---------------");
		
	
		
		
		//JavaRDD<LocGPS> data = spark.sparkContext().textFile("hdfs://beetlejuice:9000/user/antborde/cut",1).toJavaRDD().map(f);
		/*JavaPairRDD<Object, Iterable<LocGPS>> dataByKey = 
				spark.sparkContext().textFile("hdfs://beetlejuice:9000/user/antborde/cut.txt",1).toJavaRDD().map((x)->
				{
					String[] parts = x.split(",");
					LocGPS loc = new LocGPS(Double.parseDouble(parts[0]),Double.parseDouble(parts[1]),Integer.parseInt(parts[2]));
					return loc;
					
				}).groupBy((x)->
				{
					return x.getKey();
				});
		
		System.out.println(dataByKey.first());*/
		
		
		
		
		
		
	}
	

	
	
	
	public static double getSize() {
		return sizeTile;
	}
	
	public static void setSize(int size) {
		sizeTile=size;
		latTile = sizeTile/METER_PER_DEG_EQUATEUR;
	}
	
	public static double getLatTile() {
		return latTile;
	}

}

