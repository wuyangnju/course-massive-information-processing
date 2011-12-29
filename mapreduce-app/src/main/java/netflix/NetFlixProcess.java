package netflix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NetFlixProcess {
	public static class NetFlixProcessMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text movieId = new Text();
		private Text outputValue = new Text();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			movieId.set(((FileSplit) context.getInputSplit()).getPath()
					.getName().substring(3, 10));
			String[] infos = value.toString().split(",");
			if (infos.length != 3) {
				return;
			}
			outputValue.set(infos[0] + ":" + infos[1]);
			context.write(movieId, outputValue);
		}

	}

	public static class NetFlixProcessReducer extends
			Reducer<Text, Text, Text, Movie> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Movie movie = new Movie();
			movie.setMovieId(key.toString());
			for (Text value : values) {
				String[] infos = value.toString().split(":");
				movie.addRating(infos[0], Double.parseDouble(infos[1]));
			}
			movie.setWeight(1);
			context.write(key, movie);
		}

	}

	public static void runMrJob(Configuration conf, String in, String out)
			throws Exception {
		Job job = new Job(conf);
		job.setJobName(NetFlixProcess.class.getName());
		job.setJarByClass(NetFlixProcess.class);

		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(NetFlixProcessMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(NetFlixProcessReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Movie.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String in = "exp4/in";
		String out = "exp4/out";
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
