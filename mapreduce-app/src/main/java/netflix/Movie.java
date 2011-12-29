package netflix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class Movie implements Writable {

	private String movieId;
	private List<String> canopyCenters = new ArrayList<String>();
	private Map<String, Double> ratings = new LinkedHashMap<String, Double>();
	private int weight;

	public Movie() {
	}

	public String getMovieId() {
		return movieId;
	}

	public void setMovieId(String movieId) {
		this.movieId = movieId;
	}

	public List<String> getCanopyCenters() {
		return canopyCenters;
	}

	public boolean hasCommonCanopy(Movie movie) {
		return !Collections.disjoint(canopyCenters, movie.canopyCenters);
	}

	public void addCanopyCenter(String center) {
		canopyCenters.add(center);
	}

	public Set<String> getAllUsers() {
		return ratings.keySet();
	}

	public int commonUserCount(Movie movie) {
		List<String> userIdsCopy = new ArrayList<String>(ratings.keySet()
				.size());
		for (String userId : ratings.keySet()) {
			userIdsCopy.add(userId.toString());
		}
		userIdsCopy.retainAll(movie.ratings.keySet());
		return userIdsCopy.size();
	}

	public boolean hasUserRated(String userId) {
		return ratings.containsKey(userId);
	}

	public double getUserRating(String userId) {
		return ratings.get(userId);
	}

	public void addRating(String userId, double rating) {
		ratings.put(userId, rating);
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public void clear() {
		movieId = "";
		canopyCenters.clear();
		ratings.clear();
		weight = 1;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		clear();
		movieId = WritableUtils.readString(input);
		int centerSize = WritableUtils.readVInt(input);
		for (int i = 0; i < centerSize; i++) {
			canopyCenters.add(WritableUtils.readString(input));
		}
		int ratingSize = WritableUtils.readVInt(input);
		for (int i = 0; i < ratingSize; i++) {
			String userId = WritableUtils.readString(input);
			double rating = Double.parseDouble(WritableUtils.readString(input));
			ratings.put(userId, rating);
		}
		weight = WritableUtils.readVInt(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		WritableUtils.writeString(output, movieId);
		WritableUtils.writeVInt(output, canopyCenters.size());
		for (String centerId : canopyCenters) {
			WritableUtils.writeString(output, centerId);
		}
		WritableUtils.writeVInt(output, ratings.size());
		for (Map.Entry<String, Double> entry : ratings.entrySet()) {
			WritableUtils.writeString(output, entry.getKey());
			WritableUtils.writeString(output, entry.getValue().toString());
		}
		WritableUtils.writeVInt(output, weight);
	}

	public Movie getCloneMovie() {
		Movie clone = new Movie();
		clone.movieId = movieId;

		List<String> canopyCentersCopy = new ArrayList<String>(
				canopyCenters.size());
		for (String userId : ratings.keySet()) {
			canopyCentersCopy.add(userId.toString());
		}
		clone.canopyCenters = canopyCentersCopy;

		Map<String, Double> ratingsCopy = new LinkedHashMap<String, Double>(
				ratings.size());
		for (Map.Entry<String, Double> rating : ratings.entrySet()) {
			ratingsCopy.put(rating.getKey(), rating.getValue());
		}
		clone.ratings = ratingsCopy;

		clone.weight = weight;
		return clone;
	}

	public static void main(String[] args) {
		Movie movie1 = new Movie();
		movie1.addRating("1", 1);
		movie1.addRating("2", 1);
		movie1.addRating("3", 1);
		Movie movie2 = new Movie();
		movie2.addRating("4", 1);
		movie2.addRating("2", 1);
		movie2.addRating("3", 1);
		System.out.println(movie1.commonUserCount(movie2));
	}
}
