	import java.io.IOException;
	import java.io.DataOutput;
	import java.io.DataInput;
	import java.util.regex.Pattern; 

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.conf.Configured;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.Writable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
	import org.apache.hadoop.util.Tool;
	import org.apache.hadoop.util.ToolRunner;

	public class WikipediaPopular extends Configured implements Tool {

		public static class WikiMapper
		// The first input of the map is always offset so it's LongWritable
		extends Mapper<LongWritable, Text, Text, LongWritable>{
			
			LongWritable view = new LongWritable();
			Text time_period=new Text();
		@Override
			public void map(LongWritable Key, Text input_string, Context context
					) throws IOException, InterruptedException {
						Pattern whitespace = Pattern.compile("\\s+");
						String[] elements=whitespace.split(input_string.toString());
						if(elements[1]=="en"||elements[2]!="Main_Page"||elements[2].startsWith("Special:")==false){
						view.set(Long.parseLong(elements[3]));
						time_period.set(elements[0]);
						}
				context.write(time_period, view);			
			}
		}

		public static class WikiReducer
		extends Reducer<Text, LongWritable, Text, LongWritable> {
			private LongWritable result = new LongWritable();

			@Override
			public void reduce(Text time_period, Iterable<LongWritable> views,
					Context context
					) throws IOException, InterruptedException {
				long max_view = 0;
				for (LongWritable view : views) {
					if(max_view<view.get()){
						max_view=view.get();
					}
				}
				result.set(max_view);
				context.write(time_period, result);
			}
		}
		


		public static void main(String[] args) throws Exception {
			int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
			System.exit(res);
		}

		@Override
		public int run(String[] args) throws Exception {
			Configuration conf = this.getConf();
			Job job = Job.getInstance(conf, "wikipedia popular");
			job.setJarByClass(WikipediaPopular.class);

			job.setInputFormatClass(TextInputFormat.class);

			job.setMapperClass(WikiMapper.class);
			job.setCombinerClass(WikiReducer.class);
			job.setReducerClass(WikiReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			TextInputFormat.addInputPath(job, new Path(args[0]));
			TextOutputFormat.setOutputPath(job, new Path(args[1]));

			return job.waitForCompletion(true) ? 0 : 1;
		}
	}








