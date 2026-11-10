package stream.scotty.core.windowFunction.builtin;

import org.junit.Assert;
import org.junit.Test;
import stream.scotty.core.windowFunction.CommutativeAggregateFunction;
import stream.scotty.core.windowFunction.InvertibleAggregateFunction;

import java.util.AbstractMap;
import java.util.Map;

public class MinMaxTest {

    @Test
    public void areCommutativeButNotInvertible() {
        Assert.assertTrue(Min.create() instanceof CommutativeAggregateFunction);
        Assert.assertFalse(Min.create() instanceof InvertibleAggregateFunction);
        Assert.assertTrue(Max.create() instanceof CommutativeAggregateFunction);
        Assert.assertFalse(Max.create() instanceof InvertibleAggregateFunction);
    }

    @Test
    public void minIntegers() {
        Min<Integer, Integer> min = Min.create();
        Integer partial = min.lift(5);
        partial = min.liftAndCombine(partial, 3);
        partial = min.liftAndCombine(partial, 8);
        Assert.assertEquals(Integer.valueOf(3), min.lower(partial));
        Assert.assertEquals(Integer.valueOf(-1), min.combine(-1, 2));
        Assert.assertEquals(Integer.valueOf(-1), min.combine(2, -1));
    }

    @Test
    public void maxIntegers() {
        Max<Integer, Integer> max = Max.create();
        Integer partial = max.lift(5);
        partial = max.liftAndCombine(partial, 3);
        partial = max.liftAndCombine(partial, 8);
        Assert.assertEquals(Integer.valueOf(8), max.lower(partial));
        Assert.assertEquals(Integer.valueOf(2), max.combine(-1, 2));
        Assert.assertEquals(Integer.valueOf(2), max.combine(2, -1));
    }

    @Test
    public void comparableValues() {
        Assert.assertEquals("apple", Min.<String>create().combine("pear", "apple"));
        Assert.assertEquals("pear", Max.<String>create().combine("pear", "apple"));
        Assert.assertEquals(Double.valueOf(0.5), Min.<Double>create().combine(0.5, 1.5));
    }

    @Test
    public void extractedValue() {
        Min<Map.Entry<String, Long>, Long> min = new Min<>(Map.Entry::getValue);
        Max<Map.Entry<String, Long>, Long> max = new Max<>(Map.Entry::getValue);
        Map.Entry<String, Long> a = new AbstractMap.SimpleEntry<>("a", 10L);
        Map.Entry<String, Long> b = new AbstractMap.SimpleEntry<>("b", 20L);
        Assert.assertEquals(Long.valueOf(10), min.lower(min.liftAndCombine(min.lift(a), b)));
        Assert.assertEquals(Long.valueOf(20), max.lower(max.liftAndCombine(max.lift(a), b)));
    }
}
