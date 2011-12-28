package pagerank;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import pagerank.PageRank.PageRankReducer;

public class WikiProcess {

	public static class WikiProcessMapper extends
			Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String line = itr.nextToken();

				// the page title
				String title = null;
				Pattern titlePattern = Pattern
						.compile("^&lttitle&gt.+?&lt/title&gt");
				Matcher titleMatcher = titlePattern.matcher(line);
				if (titleMatcher.find()) {
					String titleWrap = titleMatcher.group();
					title = titleWrap.substring("&lttitle&gt".length(),
							titleWrap.indexOf("&lt/title&gt"));
				} else {
					System.err.println("title not found");
				}

				Text titleText = new Text(title);

				// the links
				Pattern linkPattern = Pattern.compile("\\[\\[.+?\\]\\]");
				Matcher linkMatcher = linkPattern.matcher(line);
				StringBuilder sb = new StringBuilder(PageRank.PAGE_LINKS);
				long linkCount = 0;
				while (linkMatcher.find()) {
					String linkWrap = linkMatcher.group();
					String link = linkWrap.substring("[[".length(),
							linkWrap.indexOf("]]") + 2);
					sb.append(link);
					linkCount++;
				}

				if (linkCount == 0) {
					return;
				}

				String links = sb.toString();
				context.write(titleText, new Text(PageRank.PAGE_EXISTS));
				context.write(titleText, new Text(PageRank.PAGE_LINKS + links));

				double pageRankContribution = 1.0 / (linkCount * 1.0);
				Text pageRankContributionText = new Text(PageRank.PAGE_RANK
						+ pageRankContribution);
				for (String link : links.split("]]")) {
					context.write(new Text(link), pageRankContributionText);
				}
			}
		}
	}

	public static class WikiProcessTestMapper extends
			Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String line = itr.nextToken();

				// the page title
				String title = null;
				Pattern titlePattern = Pattern
						.compile("^&lttitle&gt.+?&lt/title&gt");
				Matcher titleMatcher = titlePattern.matcher(line);
				if (titleMatcher.find()) {
					String titleWrap = titleMatcher.group();
					title = titleWrap.substring("&lttitle&gt".length(),
							titleWrap.indexOf("&lt/title&gt"));
				} else {
					System.err.println("title not found");
				}

				Text titleText = new Text(title);

				// the links
				Pattern linkPattern = Pattern.compile("\\[\\[.+?\\]\\]");
				Matcher linkMatcher = linkPattern.matcher(line);
				StringBuilder sb = new StringBuilder(PageRank.PAGE_LINKS);
				long linkCount = 0;
				while (linkMatcher.find()) {
					String linkWrap = linkMatcher.group();
					String link = linkWrap.substring("[[".length(),
							linkWrap.indexOf("]]") + 2);
					sb.append(link);
					linkCount++;
				}

				if (linkCount == 0) {
					return;
				}

				String links = sb.toString();
				context.write(titleText, new Text(links));
			}
		}
	}

	public static class WikiProcessTestReducer extends
			Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		};

	}

	public static void runMrJob(Configuration conf, String in, String out)
			throws Exception {
		Job job = new Job(conf);

		job.setJobName(WikiProcess.class.getName());
		job.setJarByClass(WikiProcess.class);

		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setMapperClass(WikiProcessMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}

	// private static void testRegex() throws Exception {
	// BufferedReader br = new BufferedReader(new InputStreamReader(
	// new FileInputStream("exp3/in/wiki_0.txt")));
	// String line;
	// int testCount = 0;
	// while ((line = br.readLine()) != null) {
	// String title = null;
	// Pattern titlePattern = Pattern
	// .compile("^&lttitle&gt.+?&lt/title&gt");
	// Matcher titleMatcher = titlePattern.matcher(line);
	// if (titleMatcher.find()) {
	// String titleWrap = titleMatcher.group();
	// title = titleWrap.substring("&lttitle&gt".length(),
	// titleWrap.indexOf("&lt/title&gt"));
	// }
	// System.out.print(title + ": ");
	//
	// Pattern linkPattern = Pattern.compile("\\[\\[.+?\\]\\]");
	// Matcher linkMatcher = linkPattern.matcher(line);
	// while (linkMatcher.find()) {
	// String linkWrap = linkMatcher.group();
	// String link = linkWrap.substring("[[".length(),
	// linkWrap.indexOf("]]"));
	// System.out.print(link + ", ");
	// }
	// System.out.println();
	// if (testCount++ >= 3) {
	// break;
	// }
	// }
	// br.close();
	// }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String in = "exp3/in";
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
