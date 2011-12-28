package pagerank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRankSort {

	public static class PageRankSortMapper extends
			Mapper<Object, Text, ReversedDoubleWritable, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String[] tokensInLine = itr.nextToken().split("]]");

				String title = tokensInLine[0];
				double pageRank = Double.parseDouble(tokensInLine[1]);

				context.write(new ReversedDoubleWritable(pageRank), new Text(
						title));
			}
		}
	}

	public static class PageRankSortReducer extends
			Reducer<ReversedDoubleWritable, Text, Text, DoubleWritable> {

		@Override
		protected void reduce(ReversedDoubleWritable key,
				Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			for (Text value : values) {
				context.write(value, new DoubleWritable(key.get()));
			}
		};

	}

	public static void runMrJob(Configuration conf, String in, String out)
			throws Exception {
		Job job = new Job(conf);

		job.setJobName(PageRankSort.class.getName());
		job.setJarByClass(PageRankSort.class);

		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setMapperClass(PageRankSortMapper.class);
		job.setReducerClass(PageRankSortReducer.class);

		job.setMapOutputKeyClass(ReversedDoubleWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String in = "exp3/tmp/4";
		String out = "exp3/out";
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length > 0) {
			in = args[0];
			out = args[1];
		}
		FileSystem.get(conf).delete(new Path(out), true);
		runMrJob(conf, in, out);
	}
}
