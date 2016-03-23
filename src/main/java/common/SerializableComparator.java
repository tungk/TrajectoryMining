package common;

import java.io.Serializable;
import java.util.Comparator;

/**
 * The composite comparator interface used in the application.
 * Spark require every object including comparators to be serializable.
 * @author a0048267
 * @param <T>
 */
public interface SerializableComparator<T> extends Serializable, Comparator<T> {

}
