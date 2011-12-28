package pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReversedDoubleWritable implements
		WritableComparable<ReversedDoubleWritable> {

	private double value = 0.0;

	public ReversedDoubleWritable() {

	}

	public ReversedDoubleWritable(double value) {
		set(value);
	}

	public void readFields(DataInput in) throws IOException {
		value = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(value);
	}

	public void set(double value) {
		this.value = value;
	}

	public double get() {
		return value;
	}

	/**
	 * Returns true iff <code>o</code> is a DoubleWritable with the same value.
	 */
	public boolean equals(Object o) {
		if (!(o instanceof ReversedDoubleWritable)) {
			return false;
		}
		ReversedDoubleWritable other = (ReversedDoubleWritable) o;
		return this.value == other.value;
	}

	public int hashCode() {
		return (int) Double.doubleToLongBits(value);
	}

	public int compareTo(ReversedDoubleWritable o) {
		ReversedDoubleWritable other = (ReversedDoubleWritable) o;
		return (value > other.value ? -1 : (value == other.value ? 0 : 1));
	}

	public String toString() {
		return Double.toString(value);
	}

	/** A Comparator optimized for DoubleWritable. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(ReversedDoubleWritable.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			double thisValue = readDouble(b1, s1);
			double thatValue = readDouble(b2, s2);
			return (thisValue > thatValue ? -1 : (thisValue == thatValue ? 0
					: 1));
		}
	}

	static { // register this comparator
		WritableComparator.define(ReversedDoubleWritable.class,
				new Comparator());
	}

}
