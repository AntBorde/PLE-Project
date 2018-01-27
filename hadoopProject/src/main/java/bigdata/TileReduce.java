package bigdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TileReduce {
	
	public static double sizeTile = 0;
	public static final int METER_PER_DEG_EQUATEUR = 111319; //wikipedia
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
	
	public static class TileMapper extends Mapper<LongWritable, Text, LocKey, LocGps> {

		public void setup(Context context) {
			setSize(1000);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split(",");
			LocGps loc = new LocGps(Double.parseDouble(tokens[0]),Double.parseDouble(tokens[1]),Integer.parseInt(tokens[2]));
			context.write(loc.getKey(), loc);
		}
	}

	
	
	public static class TileReducer extends Reducer <LocKey, Iterable<LocGps>, NullWritable, Text> {


		public void setup(Context context) {
		}

		public void reduce(LocKey key, Iterable<LocGps> values, Context context) throws IOException, InterruptedException {
			int x = key.getX();
			int y = key.getY();
			LocGps locAgg = new LocGps();
			int moyH=0;
			int c =0;
			
			for(LocGps loc : values) {
				c++;
				moyH += loc.getH();
			}
			
			moyH = moyH/c;
			locAgg.setH(moyH);
			locAgg.setLat(x*getLatTile());
			locAgg.setLng(y*(sizeTile / (METER_PER_DEG_EQUATEUR * Math.cos(((x*getLatTile()) * Math.PI) / 180))));
			
			context.write(NullWritable.get(), new Text(locAgg.toString()));
			
		}
	}

		
		
		public static void main(String[] args) throws Exception {
			System.out.println("test2");
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Tile");
			job.setNumReduceTasks(1);
			job.setJarByClass(TileReduce.class);
			job.setMapperClass(TileMapper.class);
			job.setMapOutputKeyClass(LocKey.class);
			job.setMapOutputValueClass(LocGps.class);
			job.setReducerClass(TileReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
