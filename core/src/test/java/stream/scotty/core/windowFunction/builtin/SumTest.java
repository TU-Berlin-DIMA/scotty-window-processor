package stream.scotty.core.windowFunction.builtin;

import org.junit.Assert;
import org.junit.Test;
import stream.scotty.core.windowFunction.CommutativeAggregateFunction;
import stream.scotty.core.windowFunction.InvertibleAggregateFunction;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.Map;

public class SumTest {

    @Test
    public void isInvertibleAndCommutative() {
        Assert.assertTrue(Sum.create() instanceof InvertibleAggregateFunction);
        Assert.assertTrue(Sum.create() instanceof CommutativeAggregateFunction);
    }

    @Test
    public void sumIntegers() {
        Sum<Integer, Integer> sum = Sum.create();
        Integer partial = sum.lift(1);
        partial = sum.liftAndCombine(partial, 2);
        partial = sum.combine(partial, sum.lift(3));
        Assert.assertEquals(Integer.valueOf(6), sum.lower(partial));
    }

    @Test
    public void invertIntegers() {
        Sum<Integer, Integer> sum = Sum.create();
        Assert.assertEquals(Integer.valueOf(4), sum.invert(10, 6));
        Assert.assertEquals(Integer.valueOf(7), sum.liftAndInvert(10, 3));
    }

    @Test
    public void keepsValueType() {
        Assert.assertEquals(Long.valueOf(5L), Sum.<Long>create().combine(2L, 3L));
        Assert.assertEquals(Double.valueOf(5.5), Sum.<Double>create().combine(2.25, 3.25));
        Assert.assertEquals(Float.valueOf(1.5f), Sum.<Float>create().invert(2.0f, 0.5f));
        Assert.assertEquals(BigInteger.valueOf(5), Sum.<BigInteger>create().combine(BigInteger.valueOf(2), BigInteger.valueOf(3)));
        Assert.assertEquals(new BigDecimal("0.3"), Sum.<BigDecimal>create().combine(new BigDecimal("0.1"), new BigDecimal("0.2")));
    }

    @Test
    public void sumExtractedValue() {
        Sum<Map.Entry<String, Integer>, Integer> sum = new Sum<>(Map.Entry::getValue);
        Integer partial = sum.lift(new AbstractMap.SimpleEntry<>("a", 4));
        partial = sum.liftAndCombine(partial, new AbstractMap.SimpleEntry<>("a", 5));
        partial = sum.liftAndInvert(partial, new AbstractMap.SimpleEntry<>("a", 4));
        Assert.assertEquals(Integer.valueOf(5), sum.lower(partial));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsMixedTypes() {
        Sum<Number, Number> sum = Sum.create();
        sum.combine(1, 2L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsUnsupportedTypes() {
        Sum<Short, Short> sum = Sum.create();
        sum.combine((short) 1, (short) 2);
    }
}
