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

public class KMeans {
	public static final String KMEANS_CENTER = "kmeans-center";

	public static class KmeansCenterMapper extends
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

		private Text centerKey = new Text();

		@Override
		protected void map(Text key, Movie movie, Context context)
				throws IOException, InterruptedException {
			Movie closestCenter = null;
			double closestDistance = Double.MAX_VALUE;
			for (Movie center : centers) {
				double _distance = Utils.kmeansDistance(movie, center);
				if (_distance < closestDistance) {
					closestCenter = center;
					closestDistance = _distance;
				}
			}
			centerKey.set(closestCenter.getMovieId() + "");
			context.write(centerKey, movie);
		}

	}

	public static class KmeansCenterCombiner extends
			Reducer<Text, Movie, Text, Movie> {

		@Override
		protected void reduce(Text key, Iterable<Movie> movies, Context context)
				throws IOException, InterruptedException {
			Movie center = Utils.average(movies);
			center.setMovieId(key.toString());
			context.write(key, center);
		}
	}

	public static class KmeansCenterReducer extends
			Reducer<Text, Movie, Text, Movie> {

		private Text centerKey = new Text();

		@Override
		protected void reduce(Text key, Iterable<Movie> movies, Context context)
				throws IOException, InterruptedException {
			Movie center = Utils.average(movies);
			center.setMovieId(key.toString() + "_"
					+ context.getConfiguration().getInt("round", 0));
			centerKey.set(center.getMovieId() + "");
			context.write(centerKey, center);
		}
	}

	public static class KmeansClusterMapper extends
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

		private Text centerKey = new Text();

		@Override
		protected void map(Text key, Movie movie, Context context)
				throws IOException, InterruptedException {
			Movie closestCenter = null;
			double closestDistance = Double.MAX_VALUE;
			for (Movie center : centers) {
				double _distance = Utils.kmeansDistance(movie, center);
				if (_distance < closestDistance) {
					closestCenter = center;
					closestDistance = _distance;
				}
			}
			centerKey.set(closestCenter.getMovieId());
			context.write(centerKey, movie);
		}

	}

	public static class KmeansClusterReducer extends
			Reducer<Text, Movie, Text, Text> {

		private Text clusterMovies = new Text();

		@Override
		protected void reduce(Text clusterId, Iterable<Movie> movies,
				Context context) throws IOException, InterruptedException {
			StringBuilder builder = new StringBuilder();
			for (Movie movie : movies) {
				builder.append(movie.getMovieId()).append(", ");
			}
			builder.delete(builder.length() - 2, builder.length());
			clusterMovies.set(builder.toString());
			context.write(clusterId, clusterMovies);
		}
	}

	public static boolean kmeansCenter(Configuration conf, String in, String out)
			throws Exception {
		int interation_num = conf.getInt("round", 0);
		Job job = new Job(conf);
		job.setJobName("Kmeans Center _" + interation_num);
		job.setJarByClass(KMeans.class);

		FileInputFormat.setInputPaths(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(KmeansCenterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Movie.class);

		job.setCombinerClass(KmeansCenterCombiner.class);

		job.setReducerClass(KmeansCenterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Movie.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		return job.waitForCompletion(true);
	}

	public static boolean kmeansCluster(Configuration conf, String in,
			String out) throws Exception {
		Job job = new Job(conf);
		job.setJobName("Kmeans Cluster");
		job.setJarByClass(KMeans.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(KmeansClusterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Movie.class);

		job.setReducerClass(KmeansClusterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		return job.waitForCompletion(true);
	}

}
