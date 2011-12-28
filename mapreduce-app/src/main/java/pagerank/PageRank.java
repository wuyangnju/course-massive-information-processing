package pagerank;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class PageRank {

	public static final String PAGE_EXISTS = "E";
	public static final String PAGE_RANK = "R";
	public static final String PAGE_LINKS = "L";

	private static final double DAMPLING = 0.85F;

	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String line = itr.nextToken();
				String[] tokensInLine = line.split("]]");

				Text titleText = new Text(tokensInLine[0]);
				context.write(titleText, new Text(PageRank.PAGE_EXISTS));

				int pageRankStart = line.indexOf("]]");
				int linksStart = line.indexOf("]]", pageRankStart + 2) + 2;
				Text linksText = new Text(PageRank.PAGE_LINKS
						+ line.substring(linksStart));
				context.write(titleText, linksText);

				double pageRank = Double.parseDouble(tokensInLine[1]);
				double pageRankContribution = pageRank
						/ ((tokensInLine.length - 2) * 1.0);
				Text pageRankContributionText = new Text(PageRank.PAGE_RANK
						+ pageRankContribution);
				for (int i = 2; i < tokensInLine.length; i++) {
					context.write(new Text(tokensInLine[i]),
							pageRankContributionText);
				}
			}
		}
	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			boolean isExistingWikiPage = false;
			double pageRankSum = 0;
			String links = null;

			for (Text value : values) {
				String valueString = value.toString();

				if (valueString.equals(PageRank.PAGE_EXISTS)) {
					isExistingWikiPage = true;
					continue;
				} else if (valueString.startsWith(PageRank.PAGE_LINKS)) {
					links = valueString.substring(1);
					continue;
				} else if (valueString.startsWith(PageRank.PAGE_RANK)) {
					pageRankSum += Double.parseDouble(valueString.substring(1));
				} else {
					System.out.println("map output error!");
				}
			}

			if (!isExistingWikiPage)
				return;
			double newPageRank = DAMPLING * pageRankSum + (1 - DAMPLING);

			context.write(new Text(key.toString() + "]]" + newPageRank),
					new Text("]]" + links));
		}
	}

	public static void runMrJob(Configuration conf, String in, String out)
			throws Exception {
		Job job = new Job(conf);

		job.setJobName(PageRank.class.getName());
		job.setJarByClass(PageRank.class);

		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String in = "exp3/in";
		String out = "exp3/out";
		String tmp = "exp3/tmp";
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length > 0) {
			in = args[0];
			out = args[1];
			tmp = args[2];
		}
		FileSystem.get(conf).delete(new Path(out), true);
		FileSystem.get(conf).delete(new Path(tmp), true);

		WikiProcess.runMrJob(conf, in, tmp + "/0");
		int i = 0;
		for (i = 0; i < 4; i++) {
			PageRank.runMrJob(conf, tmp + "/" + i, tmp + "/" + (i + 1));
			FileSystem.get(conf).delete(new Path(tmp + "/" + i), true);
		}
		PageRankSort.runMrJob(conf, tmp + "/" + i, out);
		FileSystem.get(conf).delete(new Path(tmp + "/" + i), true);
	}
}
