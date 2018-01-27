package bigdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class TileReduce {
	public enum WCP {
		nb_cities, nb_pop, total_pop
	};

	public static double sizeTile = 0;
	public static final double METER_PER_DEG_EQUATEUR = 111319; //wikipedia
	public static double latTile = 0;
	
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
	
	
	
	
	
	public static class TileMapper extends Mapper<LongWritable, Text, LocKey, Elevation> {
		int size;
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			size = conf.getInt("size", 1000);
			setSize(size);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.getCounter(WCP.nb_cities).increment(1);
			String tokens[] = value.toString().split(",");
			Long h = Long.parseLong(tokens[2]);
			Double lat = Double.parseDouble(tokens[0]);
			Double lng = Double.parseDouble(tokens[1]);
			LocKey k = null;
			Elevation hei = new Elevation(h);
			if (lat < 60 && lat > 45 && lng > 0 && lng < 15) {
				k = LocGps.getKey(lat, lng);
				if (h < 9000 && tokens.length == 3) {
					context.getCounter(WCP.nb_pop).increment(1);
					context.write(k, hei);
				}
			}
			return;
		}
	}
		

	/*public static class Combiner extends Reducer<LocKey, Iterable<Height>, LocKey, Height> {
		public void setup(Context context) {
		}

		public void reduce(LocKey key, Iterable<Height> values, Context context)
				throws IOException, InterruptedException {
			try {
				long moyH = 0;
				int c = 0;
				for (Height h : values) {
					c++;
					moyH += h.getHeight();
				}

			} catch (Exception e) {
				return;
			}

		}
	}*/
	
	
	public static class myReducer extends Reducer <LocKey, Iterable<Elevation>, Text, Text> {
		
		int size;
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			size = conf.getInt("size", 1000);
			setSize(size);
		}
		
		public void reduce(LocKey key, Iterable<Elevation> values, Context context) throws IOException, InterruptedException {
			
			//Text t = new Text("tamere");
			
			//context.write(new Text(key.toString()), t);
			
			
			/*LocGps locAgg = new LocGps();
			long moyH=0;
			int c =0;
			
			for(Height h : values) {
					c += h.getPoids();
					moyH += h.getHeight();
				}
			
			moyH = moyH/c;
			if(moyH <9000)
				locAgg.setH((int)moyH);
			else
				locAgg.setH(-1);
			locAgg.generateLocTileCenter(key);
			
			context.write(new Text(key.toString()), new Text(locAgg.toString()));*/
			return;
			
		}
	}

		
		
		public static void main(String[] args) throws Exception {
			System.out.println("test2");
			setSize(1000);
			System.out.println(getLatTile());
			Configuration conf = new Configuration();
			conf.setInt("size", 1000);
			Job job = Job.getInstance(conf, "Tile");
			//job.setNumReduceTasks(20);
			job.setJarByClass(TileReduce.class);
			job.setMapperClass(TileMapper.class);
			job.setMapOutputKeyClass(LocKey.class);
			job.setMapOutputValueClass(bigdata.Elevation.class);
			job.setReducerClass(myReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		

			System.exit(job.waitForCompletion(true) ? 0 : 1);
			Counters count = job.getCounters();
		    Counter nb_pop = count.findCounter(WCP.nb_pop);
		    Counter nb_cities = count.findCounter(WCP.nb_cities);
		    System.out.println(nb_pop.getDisplayName() + ":" + nb_pop.getValue());
		    System.out.println(nb_cities.getDisplayName() + ":" + nb_cities.getValue());
			
			/*setSize(1000);
			System.out.println("latTile : " + getLatTile());
			LocGps locAgg = new LocGps();
			double l = 50.21646398598;
			double lo = 7.26876543685;
			int h = 1100;
			Height hei = new Height(1100,5);
			LocKey key = LocGps.getKey(l, lo);
			System.out.println(key.toString());
			
			locAgg.setH((int)hei.getHeight());
			locAgg.generateLocTileCenter(key);
			System.out.println(locAgg.toString());*/
			
			
			/*int x = key.getX();
			int y = key.getY();
			
			locAgg.setLat((double)x*App.getLatTile());
			locAgg.setLng((double)y*(App.sizeTile / (App.METER_PER_DEG_EQUATEUR * Math.cos((locAgg.getLat()*(App.getLatTile()) * Math.PI) / 180))));
			System.out.println(locAgg.toString());*/
			
		}
	}
