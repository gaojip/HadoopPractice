import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class WordCount_E extends Configured implements Tool
{

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			for (String token : value.toString().split("\\s+"))
			{
				word.set(token);
				context.write(word, one);
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, NullWritable, NullWritable, IntWritable>
	{
		private IntWritable result = new IntWritable();
		private int count;
		
		@Override
		protected void setup(Reducer<Text, NullWritable, NullWritable, IntWritable>.Context context) throws IOException, InterruptedException {
			result = new IntWritable(0);
			count = 0;
		}
		
		
		@Override
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
		{
			count++;
		}
		
		@Override
		protected void cleanup(Reducer<Text, NullWritable, NullWritable, IntWritable>.Context context) throws IOException, InterruptedException {
			result.set(count);
			context.write(NullWritable.get(), result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new WordCount_E(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(WordCount_E.class);
		
		FileSystem fileSystem = FileSystem.get(getConf());
		Path path = new Path(args[1]);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

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