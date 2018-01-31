package bigdata;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class HbaseConnexion {
	
	public static void InitTable(int nbZoom) throws IOException {
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
	
	public static void writeBD(JavaSparkContext context, JavaPairRDD<Tuple2<Integer, Integer>, Canvas> locs) {
		locs.foreachPartition(it -> {
			Connection conn = ConnectionFactory.createConnection();
			Table table = conn.getTable(Const.TABLENAME);
			while (it.hasNext()) {
				Tuple2<Tuple2<Integer, Integer>, Canvas> loc = it.next();
				
				int keyX = loc._1._1;
				int keyY = loc._1._2;
				Canvas img = loc._2;
				
				byte[] fam = Bytes.toBytes("zoom"+img.getNbReduce());
				
				String id =("z"+(img.getNbReduce())+","+keyX+","+keyY);
				Put row = new Put(Bytes.toBytes(id.toString()));
				row.addColumn(fam, Const.COLX, Bytes.toBytes(new Integer(keyX).toString()));
				row.addColumn(fam, Const.COLY, Bytes.toBytes(new Integer(keyY).toString()));
				row.addColumn(fam, Const.COLVALUE, img.image);
				table.put(row);
			}
			conn.close();
		});
	}

}
