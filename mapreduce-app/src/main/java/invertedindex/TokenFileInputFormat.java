package invertedindex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class TokenFileInputFormat extends FileInputFormat<Text, Text> {

	@Override
	protected boolean isSplitable(JobContext ctx, Path filename) {
		return false;
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext ctx) throws IOException, InterruptedException {
		return new TokenRecordReader();
	}

	public static class TokenRecordReader extends RecordReader<Text, Text> {
		private long start;
		private long pos;
		private long end;
		private LineReader in;
		private int maxLineLength;
		private Text line;
		private Text key = null;
		private Text value = null;
		private StringTokenizer tokens = null;
		private String fileID = null;

		@Override
		public void initialize(InputSplit genericSplit,
				TaskAttemptContext context) throws IOException {
			FileSplit split = (FileSplit) genericSplit;
			Configuration job = context.getConfiguration();
			this.maxLineLength = job.getInt(
					"mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();
			fileID = file.getName();
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());
			in = new LineReader(fileIn, job);
			this.pos = start;
			line = new Text();
			key = new Text();
			value = new Text();
		}

		@Override
		public boolean nextKeyValue() throws IOException {
			boolean splitEnds = false;
			while (tokens == null || !tokens.hasMoreTokens()) {
				int lineSize = in.readLine(line, maxLineLength, Math.max(
						(int) Math.min(Integer.MAX_VALUE, end - pos),
						maxLineLength));
				if (lineSize == 0) {
					splitEnds = true;
					break;
				}
				pos += lineSize;
				tokens = new StringTokenizer(line.toString(),
						InvertedIndex.STRINGTOKENIZER_DELIM);
			}
			if (splitEnds) {
				key = null;
				value = null;
				line = null;
				tokens = null;
				return false;
			} else
				return true;
		}

		@Override
		public Text getCurrentKey() {
			key.set(tokens.nextToken());
			return key;
		}

		@Override
		public Text getCurrentValue() {
			value.set(fileID);
			return value;
		}

		@Override
		public float getProgress() {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - start) / (float) (end - start));
			}
		}

		@Override
		public synchronized void close() throws IOException {
			if (in != null) {
				in.close();
			}
		}
	}

	public static void main(String[] args) throws IOException {
		String fn = "exp1/in/20417.txt";
		Configuration conf = new Configuration();
		FileSplit split = new FileSplit(new Path(fn), 0, 10000000, null);
		TokenRecordReader irr = new TokenRecordReader();
		TaskAttemptContext ctx = new TaskAttemptContext(conf,
				new TaskAttemptID("hello", 12, true, 12, 12));
		irr.initialize(split, ctx);
		while (irr.nextKeyValue()) {
			System.out.println(irr.getCurrentKey() + ": "
					+ irr.getCurrentValue());
		}
	}
}
