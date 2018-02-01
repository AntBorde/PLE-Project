package bigdata;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App2 implements Serializable {

	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws IOException {

		HbaseConnexion.InitTable(Const.NB_ZOOM);

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		
		
		/**
		 * Initialise le premier RDD  en filtrant les bonnes valeurs et en Attribuant la premiere clé.
		 */
		JavaPairRDD<Tuple2<Integer, Integer>, LocGPS> firstRDD = context
				.textFile("hdfs://beetlejuice:9000/raw_data/dem3_lat_lng.txt").flatMap((x) -> {
					short nbReduce = 0;
					List<LocGPS> res = new ArrayList<LocGPS>();
					String[] parts = x.split(",");
					double latitude = Double.parseDouble(parts[0]);
					double longitude = LocGPS.correctionLongitude(Double.parseDouble(parts[1]));
					int hauteur = Integer.parseInt(parts[2]);

					if (latitude < 90 && latitude > -90 && longitude < 180 && longitude > -180 && hauteur < 9000
							&& hauteur > 0) {
						res.add(new LocGPS(latitude + 90, longitude + 180, hauteur, nbReduce));
					}
					return res.iterator();
					
				}).mapToPair((x) -> {
					LocGPS loc = x;
					Tuple2<Integer, Integer> key = loc.getKey(Const.FIRST_AGGREGATION_SIZE);
					Tuple2<Tuple2<Integer, Integer>, LocGPS> val = new Tuple2<Tuple2<Integer, Integer>, LocGPS>(key,
							loc);
					return val;
				}).cache();
		
		

		for (int nbReduction = 0; nbReduction < Const.NB_ZOOM; nbReduction++) {
			

			JavaPairRDD<Tuple2<Integer, Integer>, Iterable<LocGPS>> tmpCanvas = firstRDD.mapToPair((x) -> {
				Tuple2<Integer, Integer> key = new Tuple2<Integer, Integer>(x._1._1 / 256, x._1._2 / 256);
				Tuple2<Tuple2<Integer, Integer>, LocGPS> val = new Tuple2<Tuple2<Integer, Integer>, LocGPS>(key, x._2);
				return val;
			}).groupByKey();

			JavaPairRDD<Tuple2<Integer, Integer>, Canvas> canvas = tmpCanvas.mapValues((x) -> {
				short[] values = new short[Const.CANVAS_SIZE];
				Tuple2<Integer, Integer> key;

				int i;
				for (LocGPS loc : x) {
					if (loc.getNbReduce() == 0) {
						key = loc.getKey(Const.FIRST_AGGREGATION_SIZE);
					} else
						key = loc.getKey(Const.SIZE);
					i = ((key._1 % 256) * 256) + (key._2 % 256);
					values[i] = (short) loc.getH();
				}
				ByteBuffer byteBuf = ByteBuffer.allocate(Const.CANVAS_SIZE * 2);
				for (int j = 0; j < Const.CANVAS_SIZE; j++) {
					byteBuf.putShort(values[j]);
				}
				byte[] image = byteBuf.array();
				return new Canvas((short) 0, image);
			});
			
			

			HbaseConnexion.writeBD(context, canvas);

			JavaPairRDD<Tuple2<Integer, Integer>, Iterable<LocGPS>> locRDD = firstRDD.mapToPair((x) -> {
				LocGPS loc = x._2;
				Tuple2<Integer, Integer> key = loc.getKey(Const.SIZE);
				Tuple2<Tuple2<Integer, Integer>, LocGPS> val = new Tuple2<Tuple2<Integer, Integer>, LocGPS>(key, x._2);
				return val;
			}).groupByKey();

			firstRDD = locRDD.mapValues((x) -> {
				int moyH = 0;
				int c = 0;
				for (LocGPS loc : x) {
					moyH += loc.getH();
					c++;
				}
				moyH = moyH / c;
				Iterator<LocGPS> it = x.iterator();
				if (it.hasNext()) {
					LocGPS loc = it.next();
					Tuple2<Integer, Integer> key = loc.getKey(Const.SIZE);
					return new LocGPS(key._1, key._2, moyH, loc.getNbReduce());
				}
				return new LocGPS(1, 1, -1, (short) 0);
			}).cache();
		}
		context.close();

	}

}
