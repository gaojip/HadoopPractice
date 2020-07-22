import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortTemp extends Configured implements Tool
{
	
	public static class StationGroupingComparator extends WritableComparator {
		protected StationGroupingComparator() {
			super(SortBean.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			SortBean sortBeanA = (SortBean)a;
			SortBean sortBeanB = (SortBean)b;
			return sortBeanA.getStationID().compareTo(sortBeanB.getStationID());
		}
	}

	public static class SortTempMapper extends Mapper<LongWritable, Text, SortBean, IntWritable>
	{
		private IntWritable year = new IntWritable();
		private SortBean sortBean = new SortBean();
		private Map<Integer, Integer> pairMap = new HashMap<>();
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			try {
				int temp = Integer.parseInt(line.substring(87, 92).toString());
				int lineYear = Integer.parseInt(line.substring(15, 19));
				String stationID = line.substring(4, 10) + "-" + line.substring(10, 15);
				sortBean.set(stationID, temp);
				year.set(lineYear);
				if (!(pairMap.containsKey(sortBean.hashCode()) && pairMap.get(sortBean.hashCode()) == lineYear)) {
					context.write(sortBean, year);
					pairMap.put(sortBean.hashCode(), lineYear);
				}
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	public static class SortTempReducer extends Reducer<SortBean, IntWritable, SortBean, IntWritable>
	{
		@Override
		public void reduce(SortBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			for (IntWritable val : values)
			{
				context.write(key, val);
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new SortTemp(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "SortTemp");
		job.setJarByClass(SortTemp.class);
		
		FileSystem fileSystem = FileSystem.get(getConf());
		Path path = new Path(args[1]);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}

		job.setGroupingComparatorClass(StationGroupingComparator.class);
		job.setMapperClass(SortTempMapper.class);
		job.setReducerClass(SortTempReducer.class);

		job.setOutputKeyClass(SortBean.class);
		job.setOutputValueClass(IntWritable.class);
		
//		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}