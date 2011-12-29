package netflix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

public class NetFlix {

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

		Canopy.canopyCenter(conf, tmp + "/netflix", tmp + "/"
				+ Canopy.CANOPY_CENTER);

		Canopy.canopyCluster(conf, tmp + "/netflix", tmp + "/"
				+ Canopy.CANOPY_CLUSTER, tmp + "/" + Canopy.CANOPY_CENTER);

		conf.setInt("round", 0);
		conf.set("center", tmp + "/" + Canopy.CANOPY_CENTER);
		KMeans.kmeansCenter(conf, tmp + "/" + Canopy.CANOPY_CLUSTER, tmp + "/"
				+ KMeans.KMEANS_CENTER + "_0");
		int i;
		for (i = 1; i <= 5; i++) {
			System.out.println(i);
			conf.setInt("round", i);
			conf.set("center", tmp + "/" + KMeans.KMEANS_CENTER + "_" + (i - 1));
			KMeans.kmeansCenter(conf, tmp + "/" + KMeans.KMEANS_CENTER + "_"
					+ (i - 1), tmp + "/" + KMeans.KMEANS_CENTER + "_" + i);
		}

		conf.set("center", tmp + "/" + KMeans.KMEANS_CENTER + "_" + (i - 1));
		KMeans.kmeansCluster(conf, tmp + "/" + Canopy.CANOPY_CLUSTER, out);
	}
}
