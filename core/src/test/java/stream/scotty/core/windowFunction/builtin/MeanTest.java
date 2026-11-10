/*
 * Copyright The Scotty Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package stream.scotty.core.windowFunction.builtin;

import org.junit.Assert;
import org.junit.Test;
import stream.scotty.core.windowFunction.CommutativeAggregateFunction;
import stream.scotty.core.windowFunction.InvertibleAggregateFunction;

import java.util.AbstractMap;
import java.util.Map;

public class MeanTest {

    @Test
    public void isInvertibleAndCommutative() {
        Assert.assertTrue(Mean.create() instanceof InvertibleAggregateFunction);
        Assert.assertTrue(Mean.create() instanceof CommutativeAggregateFunction);
    }

    @Test
    public void meanIntegers() {
        Mean<Integer> mean = Mean.create();
        Mean.SumCount partial = mean.lift(1);
        Assert.assertEquals(new Mean.SumCount(1, 1), partial);
        partial = mean.liftAndCombine(partial, 2);
        partial = mean.liftAndCombine(partial, 4);
        Assert.assertEquals(new Mean.SumCount(7, 3), partial);
        // no integer division
        Assert.assertEquals(7.0 / 3, mean.lower(partial), 0.0);
    }

    @Test
    public void combinePartials() {
        Mean<Double> mean = Mean.create();
        Mean.SumCount combined = mean.combine(new Mean.SumCount(3.0, 2), new Mean.SumCount(5.0, 2));
        Assert.assertEquals(new Mean.SumCount(8.0, 4), combined);
        Assert.assertEquals(2.0, mean.lower(combined), 0.0);
    }

    @Test
    public void invert() {
        Mean<Integer> mean = Mean.create();
        Mean.SumCount partial = new Mean.SumCount(10, 4);
        Assert.assertEquals(new Mean.SumCount(6, 2), mean.invert(partial, new Mean.SumCount(4, 2)));
        Assert.assertEquals(new Mean.SumCount(9, 3), mean.liftAndInvert(partial, 1));
        Assert.assertEquals(3.0, mean.lower(mean.liftAndInvert(partial, 1)), 0.0);
    }

    @Test
    public void extractedValue() {
        Mean<Map.Entry<String, Integer>> mean = new Mean<>(Map.Entry::getValue);
        Mean.SumCount partial = mean.lift(new AbstractMap.SimpleEntry<>("a", 1));
        partial = mean.liftAndCombine(partial, new AbstractMap.SimpleEntry<>("a", 2));
        Assert.assertEquals(1.5, mean.lower(partial), 0.0);
    }
}
