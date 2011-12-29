package invertedindex;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {

	public static final String STRINGTOKENIZER_DELIM = " \t\n\r\f~`!@#$%^&*()_-+={[}]|\\:;\"'<,>.?/";
	public static final String FREQUENT_WORDS_FILE = "frequent-words.txt";

	public static class InvertedIndexMapper extends
			Mapper<Text, Text, Text, Text> {
		private Set<String> frequentWords = new LinkedHashSet<String>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new FileInputStream(FREQUENT_WORDS_FILE)));
			String line;
			while ((line = br.readLine()) != null) {
				frequentWords.addAll(Arrays.asList(line.split(" ")));
			}
			br.close();
		};

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			if (!frequentWords.contains(key.toString())) {
				context.write(key, value);
			}
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Set<String> fileIds = new LinkedHashSet<String>();
			for (Text value : values) {
				fileIds.addAll(Arrays.asList(value.toString().split(", ")));
			}

			StringBuilder fileIdsAsString = new StringBuilder("");
			for (String fileId : fileIds) {
				fileIdsAsString.append(fileId + ", ");
			}
			fileIdsAsString.delete(fileIdsAsString.length() - 2,
					fileIdsAsString.length());

			result.set(fileIdsAsString.toString());
			context.write(key, result);
		}
	}

	public static void runMrJob(Configuration conf, String in, String out)
			throws Exception {
		Job job = new Job(conf);

		job.setJobName(InvertedIndex.class.getName());
		job.setJarByClass(InvertedIndex.class);

		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setInputFormatClass(TokenFileInputFormat.class);

		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String in = "exp2/in";
		String out = "exp2/out";
		String tmp = "exp2/tmp";
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length > 0) {
			in = args[0];
			out = args[1];
			tmp = args[2];
		}
		FileSystem.get(conf).delete(new Path(out), true);
		FileSystem.get(conf).delete(new Path(tmp), true);

		FrequentWord.runMrJob(conf, in, tmp);

		BufferedReader br = new BufferedReader(new InputStreamReader(FileSystem
				.get(conf).open(new Path(tmp + "/part-r-00000"))));
		PrintWriter pw = new PrintWriter(FileSystem.get(conf).create(
				new Path(tmp + "/frequent-words.txt")));
		String line;
		while ((line = br.readLine()) != null) {
			if (Long.parseLong(line.split("\t")[1]) >= 20) {
				pw.write(line.split("\t")[0] + " ");
			}
		}
		pw.close();
		br.close();

		DistributedCache.createSymlink(conf);

		String uri = new Path(tmp + "/" + FREQUENT_WORDS_FILE).toUri()
				.toString() + "#" + FREQUENT_WORDS_FILE;

		DistributedCache.addCacheFile(new URI(uri), conf);

		InvertedIndex.runMrJob(conf, in, out);
	}
}
