package netflix;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Utils {

	public static boolean isStronglyAssociatedToCenter(Movie movie, Movie center) {
		return center.commonUserCount(movie) >= 8;
	}

	public static boolean isWeaklyAssociatedToCenter(Movie movie, Movie center) {
		return center.commonUserCount(movie) >= 2;
	}

	public static double kmeansDistance(Movie movie1, Movie movie2) {
		double distance = 0;
		if (!movie1.hasCommonCanopy(movie2)) {
			return Integer.MAX_VALUE;
		}
		Map<String, Double> diffRating = new HashMap<String, Double>();
		for (String userId : movie1.getAllUsers()) {
			diffRating.put(userId.toString(), movie1.getUserRating(userId));
		}
		for (String userId : movie2.getAllUsers()) {
			diffRating.put(userId.toString(), movie2.getUserRating(userId)
					- (diffRating.containsKey(userId) ? diffRating.get(userId)
							: 0));
		}
		for (double value : diffRating.values()) {
			distance += value * value;
		}
		return distance;
	}

	public static Movie average(Iterable<Movie> movies) {
		Set<String> canopyCenters = new HashSet<String>();
		Map<String, Double> diffRating = new HashMap<String, Double>();
		int size = 0;
		for (Movie movie : movies) {
			for (String userId : movie.getAllUsers()) {
				diffRating.put(
						userId.toString(),
						movie.getUserRating(userId)
								* movie.getWeight()
								+ (diffRating.containsKey(userId) ? diffRating
										.get(userId) : 0));
			}
			if (canopyCenters.size() == 0) {
				canopyCenters.addAll(movie.getCanopyCenters());
			} else {
				canopyCenters.retainAll(movie.getCanopyCenters());
			}
			size += movie.getWeight();
		}
		Movie average = new Movie();
		for (String canopyCenterId : canopyCenters) {
			average.addCanopyCenter(canopyCenterId);
		}
		for (Map.Entry<String, Double> rinfo : diffRating.entrySet()) {
			average.addRating(rinfo.getKey(), rinfo.getValue() / (double) size);
		}
		average.setWeight(size);
		return average;
	}

}
