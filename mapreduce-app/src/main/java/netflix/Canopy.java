package netflix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Canopy {
	public static final String CENTER_KEY = "center";

	public static final String CANOPY_CENTER = "canopy-center";
	public static final String CANOPY_CLUSTER = "canopy-cluster";

	public static class CanopyCenterMapper extends
			Mapper<Text, Movie, Text, Movie> {

		private final Text centerKey = new Text(CENTER_KEY);
		private List<Movie> centers = new ArrayList<Movie>();

		@Override
		protected void map(Text key, Movie movie, Context context)
				throws IOException, InterruptedException {
			boolean stronglyAssociated = false;
			for (Movie center : centers) {
				if (Utils.isStronglyAssociatedToCenter(movie, center)) {
					stronglyAssociated = true;
					break;
				}
			}
			if (!stronglyAssociated) {
				centers.add(movie.getCloneMovie());
				context.write(centerKey, movie);
			}
		}

	}

	public static class CanopyCenterReducer extends
			Reducer<Text, Movie, Text, Movie> {

		private Text centerKey = new Text();
		private List<Movie> centers = new ArrayList<Movie>();

		@Override
		protected void reduce(Text key, Iterable<Movie> movies, Context context)
				throws IOException, InterruptedException {
			for (Movie movie : movies) {
				boolean stronglyAssociated = false;
				for (Movie center : centers) {
					if (Utils.isStronglyAssociatedToCenter(movie, center)) {
						stronglyAssociated = true;
					}
				}
				if (!stronglyAssociated) {
					centers.add(movie.getCloneMovie());
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (int i = 0; i < centers.size(); i++) {
				Movie movie = centers.get(i);
				for (int j = i; j < centers.size(); j++) {
					Movie center = centers.get(j);
					if (Utils.isWeaklyAssociatedToCenter(movie, center)) {
						movie.addCanopyCenter(center.getMovieId());
					}
				}
				centerKey.set(String.valueOf(movie.getMovieId()));
				context.write(centerKey, movie);
			}
		}

	}

	public static class CanopyClusterMapper extends
			Mapper<Text, Movie, Text, Movie> {

		protected List<Movie> centers = new ArrayList<Movie>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Path centerPath = new Path(context.getConfiguration().get("center"));
			Configuration conf = context.getConfiguration();
			FileSystem fs = centerPath.getFileSystem(conf);
			FileStatus[] files = fs.listStatus(centerPath, new PathFilter() {
				@Override
				public boolean accept(Path path) {
					return path.getName().startsWith("part");
				}
			});

			for (FileStatus file : files) {
				Reader reader = new Reader(fs, file.getPath(),
						new Configuration());
				Text key = new Text();
				Movie value = new Movie();
				while (reader.next(key, value)) {
					centers.add(value);
					value = new Movie();
				}
				reader.close();
			}
		}

		@Override
		protected void map(Text key, Movie movie, Context context)
				throws IOException, InterruptedException {
			for (Movie center : centers) {
				if (Utils.isWeaklyAssociatedToCenter(movie, center)) {
					movie.addCanopyCenter(center.getMovieId());
				}
			}
			context.write(key, movie);
		}

	}

	public static boolean canopyCenter(Configuration conf, String in, String out)
			throws Exception {
		Job job = new Job(conf);
		job.setJobName("Canopy Center");
		job.setJarByClass(Canopy.class);

		FileInputFormat.setInputPaths(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(CanopyCenterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Movie.class);

		job.setReducerClass(CanopyCenterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Movie.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		return job.waitForCompletion(true);
	}

	public static boolean canopyCluster(Configuration conf, String in,
			String out, String center) throws Exception {
		conf.set("center", center);

		Job job = new Job(conf);
		job.setJobName("Canopy Cluster");
		job.setJarByClass(Canopy.class);

		FileInputFormat.setInputPaths(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(CanopyClusterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Movie.class);

		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Movie.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String in = "exp4/in";
		String out = "exp4/out";
		String tmp = "exp4/tmp";
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length > 0) {
			in = args[0];
			out = args[1];
			tmp = args[2];
		}
		FileSystem.get(conf).delete(new Path(out), true);
		FileSystem.get(conf).delete(new Path(tmp), true);
		NetFlixProcess.runMrJob(conf, in, tmp + "/netflix");
		canopyCenter(conf, tmp + "/netflix", tmp + "/" + CANOPY_CENTER);
		canopyCluster(conf, tmp + "/netflix", tmp + "/" + CANOPY_CLUSTER, tmp
				+ "/" + CANOPY_CENTER);
	}

}
