package stream.scotty.core.windowFunction.builtin;

import stream.scotty.core.windowFunction.CloneablePartialStateFunction;
import stream.scotty.core.windowFunction.CommutativeAggregateFunction;
import stream.scotty.core.windowFunction.InvertibleAggregateFunction;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Exact quantile of the values in a window, e.g. the median for {@code Quantile.create(0.5)}.
 * <p>
 * The q-quantile of n values is the value at rank ceil(q * n) in ascending order, counting from 1
 * (nearest-rank method). The result is always one of the values in the window. For example,
 * the 0.5-quantile of 1, 2, 3, 4 is 2, and of 1, 2, 3, 4, 5 it is 3.
 * q * n is computed with decimal arithmetic, so the 0.3-quantile of 10 values is the 3rd smallest value.
 * q must be in (0, 1]: q = 1 is the maximum, use {@link Min} for the minimum.
 * <p>
 * Works for all {@link Comparable} values, e.g. numbers or strings. Values that compare as equal are counted
 * as the same value. The partial aggregate counts how often each distinct value occurs ({@link ValueCounts}),
 * so its size grows with the number of distinct values in a slice or window.
 * Quantile is invertible and commutative. To avoid copying the counts for each tuple, combine and invert
 * update the first partial aggregate in place. Scotty clones a slice's partial aggregate with
 * {@link #clone(ValueCounts)} before it combines other slices into it, so slices are not modified.
 * The quantile of an empty partial aggregate is {@code null}.
 * <p>
 * Use {@link #create(double)} if the input tuples are the values,
 * or {@link #Quantile(double, ValueExtractor)} to use a value extracted from each input tuple.
 * @param <InputType> type of the input tuples
 * @param <ValueType> type of the values
 */
public class Quantile<InputType, ValueType extends Comparable<? super ValueType>>
        extends ValueAggregateFunction<InputType, ValueType, Quantile.ValueCounts<ValueType>, ValueType> implements
        InvertibleAggregateFunction<InputType, Quantile.ValueCounts<ValueType>, ValueType>,
        CommutativeAggregateFunction<InputType, Quantile.ValueCounts<ValueType>, ValueType>,
        CloneablePartialStateFunction<Quantile.ValueCounts<ValueType>> {

    private final double quantile;

    /**
     * @param quantile the quantile q in (0, 1], e.g. 0.5 for the median
     * @param valueExtractor extracts the value from each input tuple
     */
    public Quantile(double quantile, ValueExtractor<InputType, ValueType> valueExtractor) {
        super(valueExtractor);
        if (!(quantile > 0 && quantile <= 1)) {
            throw new IllegalArgumentException("Quantile must be in (0, 1], but was " + quantile);
        }
        this.quantile = quantile;
    }

    /**
     * @param quantile the quantile q in (0, 1], e.g. 0.5 for the median
     * @return a quantile over input tuples that are comparable values themselves.
     */
    public static <ValueType extends Comparable<? super ValueType>> Quantile<ValueType, ValueType> create(double quantile) {
        return new Quantile<>(quantile, value -> value);
    }

    @Override
    public ValueCounts<ValueType> lift(InputType inputTuple) {
        ValueCounts<ValueType> valueCounts = new ValueCounts<>();
        valueCounts.add(extract(inputTuple), 1);
        return valueCounts;
    }

    @Override
    public ValueCounts<ValueType> liftAndCombine(ValueCounts<ValueType> partialAggregate, InputType inputTuple) {
        partialAggregate.add(extract(inputTuple), 1);
        return partialAggregate;
    }

    @Override
    public ValueCounts<ValueType> combine(ValueCounts<ValueType> partialAggregate1, ValueCounts<ValueType> partialAggregate2) {
        partialAggregate1.addAll(partialAggregate2);
        return partialAggregate1;
    }

    @Override
    public ValueCounts<ValueType> liftAndInvert(ValueCounts<ValueType> partialAggregate, InputType toRemove) {
        partialAggregate.remove(extract(toRemove), 1);
        return partialAggregate;
    }

    @Override
    public ValueCounts<ValueType> invert(ValueCounts<ValueType> currentAggregate, ValueCounts<ValueType> toRemove) {
        currentAggregate.removeAll(toRemove);
        return currentAggregate;
    }

    @Override
    public ValueCounts<ValueType> clone(ValueCounts<ValueType> partialAggregate) {
        return partialAggregate.copy();
    }

    @Override
    public ValueType lower(ValueCounts<ValueType> aggregate) {
        if (aggregate.size() == 0) {
            return null;
        }
        long rank = BigDecimal.valueOf(quantile).multiply(BigDecimal.valueOf(aggregate.size()))
                .setScale(0, RoundingMode.CEILING).longValueExact();
        return aggregate.valueAtRank(rank);
    }

    /**
     * Partial aggregate of {@link Quantile}: how often each distinct value occurs, sorted by value.
     * It is modified in place by {@link Quantile}.
     * @param <ValueType> type of the values
     */
    public static final class ValueCounts<ValueType extends Comparable<? super ValueType>> implements Serializable {

        private final TreeMap<ValueType, Long> counts;
        private long size;

        public ValueCounts() {
            this(new TreeMap<>(), 0);
        }

        private ValueCounts(TreeMap<ValueType, Long> counts, long size) {
            this.counts = counts;
            this.size = size;
        }

        /**
         * @return the number of values, including duplicates.
         */
        public long size() {
            return size;
        }

        /**
         * @return how often the value occurs.
         */
        public long count(ValueType value) {
            Long count = counts.get(value);
            return count == null ? 0 : count;
        }

        /**
         * @return a read-only view of the counts of the distinct values, sorted by value.
         */
        public NavigableMap<ValueType, Long> getCounts() {
            return Collections.unmodifiableNavigableMap(counts);
        }

        void add(ValueType value, long count) {
            counts.merge(value, count, Long::sum);
            size += count;
        }

        void addAll(ValueCounts<ValueType> other) {
            for (Map.Entry<ValueType, Long> entry : other.counts.entrySet()) {
                add(entry.getKey(), entry.getValue());
            }
        }

        void remove(ValueType value, long count) {
            long current = count(value);
            if (current < count) {
                throw new IllegalArgumentException("Cannot remove " + count + " x " + value
                        + ", the value occurs " + current + " times");
            }
            if (current == count) {
                counts.remove(value);
            } else {
                counts.put(value, current - count);
            }
            size -= count;
        }

        void removeAll(ValueCounts<ValueType> other) {
            for (Map.Entry<ValueType, Long> entry : other.counts.entrySet()) {
                remove(entry.getKey(), entry.getValue());
            }
        }

        ValueCounts<ValueType> copy() {
            return new ValueCounts<>(new TreeMap<>(counts), size);
        }

        /**
         * @param rank rank in ascending order, from 1 to {@link #size()}
         * @return the value at the rank
         */
        ValueType valueAtRank(long rank) {
            // walk from the closer end, e.g. from the maximum for high quantiles
            boolean ascending = rank <= size / 2;
            Iterator<Map.Entry<ValueType, Long>> entries = ascending
                    ? counts.entrySet().iterator()
                    : counts.descendingMap().entrySet().iterator();
            long remaining = ascending ? rank : size - rank + 1;
            while (true) {
                Map.Entry<ValueType, Long> entry = entries.next();
                remaining -= entry.getValue();
                if (remaining <= 0) {
                    return entry.getKey();
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return counts.equals(((ValueCounts<?>) o).counts);
        }

        @Override
        public int hashCode() {
            return counts.hashCode();
        }

        @Override
        public String toString() {
            return "ValueCounts" + counts;
        }
    }
}
