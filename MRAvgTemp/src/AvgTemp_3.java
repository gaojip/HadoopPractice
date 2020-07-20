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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvgTemp_3 extends Configured implements Tool
{

	public static class AvgTempMapper extends Mapper<LongWritable, Text, Text, TempPair>
	{
		private Map<String, TempPair> pairMap = new HashMap<String, TempPair>();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			int temp = Integer.parseInt(line.substring(87, 92).toString());
			String year = line.substring(15, 19);
			TempPair pair = pairMap.get(year);
			if (pair == null) {
				pair = new TempPair();
				pairMap.put(year, pair);
			}
			pair.set(pair.getTemp().get() + temp, pair.getCount().get() + 1);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (String key : pairMap.keySet()) {
				Text year = new Text();
				year.set(key);
				context.write(year, pairMap.get(key));
			}
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

		int res = ToolRunner.run(conf, new AvgTemp_3(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "AvgTemp");
		job.setJarByClass(AvgTemp_3.class);
		
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