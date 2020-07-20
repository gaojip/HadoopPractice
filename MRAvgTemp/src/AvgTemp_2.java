import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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

public class AvgTemp_2 extends Configured implements Tool
{

	public static class AvgTempMapper extends Mapper<LongWritable, Text, Text, TempPair>
	{
		private Text year = new Text();
		private TempPair pair = new TempPair();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			int temp = Integer.parseInt(line.substring(87, 92).toString());
			year.set(line.substring(15, 19));
			pair.set(temp, 1);
			context.write(year, pair);
		}
	}

	public class AvgTempCombiner extends Reducer<Text, TempPair, Text, TempPair>
	{
		private TempPair pair = new TempPair();
		
		@Override
		public void reduce(Text key, Iterable<TempPair> values, Context context) throws IOException, InterruptedException
		{
			int temp = 0;
			int count = 0;
			for (TempPair val : values) {
				temp += val.getTemp().get();
				count += val.getCount().get();
			}
			pair.set(temp, count);
			context.write(key, pair);
		}
	}
	
	public static class AvgTempReducer extends Reducer<Text, TempPair, Text, DoubleWritable>
	{
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<TempPair> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			int count = 0;
			for (TempPair val : values)
			{
				count += val.getCount().get();
				sum += val.getTemp().get();
			}
			double avg = 1.0*sum/count;
			result.set(avg/10);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AvgTemp_2(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "AvgTemp");
		job.setJarByClass(AvgTemp_2.class);
		
		FileSystem fileSystem = FileSystem.get(getConf());
		Path path = new Path(args[1]);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}

		job.setMapperClass(AvgTempMapper.class);
		job.setReducerClass(AvgTempReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TempPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
//		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}