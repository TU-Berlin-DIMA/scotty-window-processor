package stream.scotty.core.windowFunction.builtin;

import org.junit.Assert;
import org.junit.Test;
import stream.scotty.core.windowFunction.CloneablePartialStateFunction;
import stream.scotty.core.windowFunction.CommutativeAggregateFunction;
import stream.scotty.core.windowFunction.InvertibleAggregateFunction;

import java.util.AbstractMap;
import java.util.Map;

public class QuantileTest {

    @SafeVarargs
    private static <T extends Comparable<? super T>> Quantile.ValueCounts<T> partial(Quantile<T, T> quantile, T... values) {
        Quantile.ValueCounts<T> partial = quantile.lift(values[0]);
        for (int i = 1; i < values.length; i++) {
            partial = quantile.liftAndCombine(partial, values[i]);
        }
        return partial;
    }

    @SafeVarargs
    private static <T extends Comparable<? super T>> T quantile(double q, T... values) {
        Quantile<T, T> quantile = Quantile.create(q);
        return quantile.lower(partial(quantile, values));
    }

    @Test
    public void isInvertibleCommutativeAndCloneable() {
        Assert.assertTrue(Quantile.create(0.5) instanceof InvertibleAggregateFunction);
        Assert.assertTrue(Quantile.create(0.5) instanceof CommutativeAggregateFunction);
        Assert.assertTrue(Quantile.create(0.5) instanceof CloneablePartialStateFunction);
    }

    @Test
    public void nearestRankMedian() {
        Assert.assertEquals(Integer.valueOf(3), quantile(0.5, 5, 1, 4, 2, 3));
        // lower median for an even number of values
        Assert.assertEquals(Integer.valueOf(2), quantile(0.5, 4, 1, 3, 2));
        Assert.assertEquals(Integer.valueOf(7), quantile(0.5, 7));
    }

    @Test
    public void quantiles() {
        Integer[] values = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
        Assert.assertEquals(Integer.valueOf(1), quantile(0.1, values));
        Assert.assertEquals(Integer.valueOf(2), quantile(0.11, values));
        // 0.3 * 10 is exactly 3 in decimal arithmetic, but 3.0000000000000004 as double
        Assert.assertEquals(Integer.valueOf(3), quantile(0.3, values));
        Assert.assertEquals(Integer.valueOf(9), quantile(0.9, values));
        Assert.assertEquals(Integer.valueOf(10), quantile(0.95, values));
    }

    @Test
    public void edgeQuantiles() {
        Assert.assertEquals(Integer.valueOf(9), quantile(1.0, 3, 9, 1, 9));
        Assert.assertEquals(Integer.valueOf(1), quantile(Double.MIN_VALUE, 3, 9, 1, 9));
        Assert.assertEquals(Integer.valueOf(5), quantile(1.0, 5));
        Assert.assertEquals(Integer.valueOf(5), quantile(Double.MIN_VALUE, 5));
    }

    @Test
    public void duplicates() {
        Quantile<Integer, Integer> median = Quantile.create(0.5);
        Quantile.ValueCounts<Integer> partial = partial(median, 2, 7, 2, 2, 7);
        Assert.assertEquals(5, partial.size());
        Assert.assertEquals(3, partial.count(2));
        Assert.assertEquals(2, partial.count(7));
        Assert.assertEquals(0, partial.count(5));
        Assert.assertEquals(2, partial.getCounts().size());
        Assert.assertEquals(Integer.valueOf(2), median.lower(partial));
        Assert.assertEquals(Integer.valueOf(7), Quantile.<Integer>create(0.8).lower(partial));
    }

    @Test
    public void combineCountsDuplicates() {
        Quantile<Integer, Integer> median = Quantile.create(0.5);
        // 5, 7, 7
        Assert.assertEquals(Integer.valueOf(7), median.lower(median.combine(partial(median, 5), partial(median, 7, 7))));
        Assert.assertEquals(Integer.valueOf(7), median.lower(median.combine(partial(median, 7, 7), partial(median, 5))));

        // 1, 1, 1, 9
        Quantile.ValueCounts<Integer> combined = median.combine(partial(median, 9), partial(median, 1, 1, 1));
        Assert.assertEquals(4, combined.size());
        Assert.assertEquals(3, combined.count(1));
        Assert.assertEquals(Integer.valueOf(1), median.lower(combined));
        Assert.assertEquals(Integer.valueOf(1), Quantile.<Integer>create(0.75).lower(combined));
        Assert.assertEquals(Integer.valueOf(9), Quantile.<Integer>create(0.76).lower(combined));
    }

    @Test
    public void combineModifiesOnlyTheFirstPartial() {
        Quantile<Integer, Integer> median = Quantile.create(0.5);
        Quantile.ValueCounts<Integer> first = partial(median, 1, 2);
        Quantile.ValueCounts<Integer> second = partial(median, 2, 3);
        Assert.assertSame(first, median.combine(first, second));
        Assert.assertEquals(partial(median, 1, 2, 2, 3), first);
        Assert.assertEquals(partial(median, 2, 3), second);
    }

    @Test
    public void cloneIsIndependent() {
        Quantile<Integer, Integer> median = Quantile.create(0.5);
        Quantile.ValueCounts<Integer> original = partial(median, 1, 2);
        Quantile.ValueCounts<Integer> clone = median.clone(original);
        Assert.assertEquals(original, clone);
        median.combine(clone, partial(median, 3, 3));
        median.liftAndCombine(clone, 1);
        median.liftAndInvert(clone, 2);
        Assert.assertEquals(partial(median, 1, 2), original);
        Assert.assertEquals(2, original.size());
        Assert.assertEquals(partial(median, 1, 1, 3, 3), clone);
    }

    @Test
    public void invert() {
        Quantile<Integer, Integer> median = Quantile.create(0.5);
        Quantile.ValueCounts<Integer> partial = partial(median, 1, 5, 5, 5, 9);
        Assert.assertEquals(Integer.valueOf(5), median.lower(partial));

        partial = median.invert(partial, partial(median, 5, 5));
        Assert.assertEquals(partial(median, 1, 5, 9), partial);
        Assert.assertEquals(3, partial.size());

        // the last 5 is removed, no zero count remains
        partial = median.liftAndInvert(partial, 5);
        Assert.assertEquals(partial(median, 1, 9), partial);
        Assert.assertFalse(partial.getCounts().containsKey(5));
        Assert.assertEquals(Integer.valueOf(1), median.lower(partial));

        partial = median.invert(partial, partial(median, 1));
        Assert.assertEquals(Integer.valueOf(9), median.lower(partial));
    }

    @Test
    public void emptyPartialHasNoQuantile() {
        Quantile<Integer, Integer> median = Quantile.create(0.5);
        Quantile.ValueCounts<Integer> partial = median.liftAndInvert(median.lift(4), 4);
        Assert.assertEquals(0, partial.size());
        Assert.assertTrue(partial.getCounts().isEmpty());
        Assert.assertNull(median.lower(partial));
        Assert.assertEquals(Integer.valueOf(6), median.lower(median.liftAndCombine(partial, 6)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invertRejectsMissingValue() {
        Quantile<Integer, Integer> median = Quantile.create(0.5);
        median.invert(partial(median, 1, 2), partial(median, 2, 2));
    }

    @Test
    public void comparableValues() {
        Assert.assertEquals("kiwi", quantile(0.5, "pear", "apple", "kiwi", "plum"));
        Assert.assertEquals(Double.valueOf(2.5), quantile(0.5, 0.5, 2.5, 7.5));
    }

    @Test
    public void extractedValue() {
        Quantile<Map.Entry<String, Integer>, Integer> median = new Quantile<>(0.5, Map.Entry::getValue);
        Quantile.ValueCounts<Integer> partial = median.lift(new AbstractMap.SimpleEntry<>("a", 30));
        partial = median.liftAndCombine(partial, new AbstractMap.SimpleEntry<>("b", 10));
        partial = median.liftAndCombine(partial, new AbstractMap.SimpleEntry<>("a", 20));
        Assert.assertEquals(Integer.valueOf(20), median.lower(partial));
        partial = median.liftAndInvert(partial, new AbstractMap.SimpleEntry<>("a", 10));
        Assert.assertEquals(Integer.valueOf(20), median.lower(partial));
        Assert.assertEquals(1, partial.count(30));
    }

    @Test
    public void rejectsInvalidQuantiles() {
        for (double q : new double[]{0.0, -0.5, 1.0000001, 2, Double.NaN, Double.POSITIVE_INFINITY}) {
            try {
                Quantile.create(q);
                Assert.fail("Accepted quantile " + q);
            } catch (IllegalArgumentException expected) {
                // expected
            }
        }
    }
}
