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

public class CountTest {

    @Test
    public void isInvertibleAndCommutative() {
        Assert.assertTrue(new Count<>() instanceof InvertibleAggregateFunction);
        Assert.assertTrue(new Count<>() instanceof CommutativeAggregateFunction);
    }

    @Test
    public void countTuples() {
        Count<String> count = new Count<>();
        Long partial = count.lift("a");
        partial = count.liftAndCombine(partial, "b");
        partial = count.liftAndCombine(partial, "c");
        Assert.assertEquals(Long.valueOf(3), count.lower(partial));
    }

    @Test
    public void combinePartials() {
        Count<String> count = new Count<>();
        Assert.assertEquals(Long.valueOf(7), count.combine(3L, 4L));
    }

    @Test
    public void invert() {
        Count<String> count = new Count<>();
        // removing a partial aggregate of three tuples
        Assert.assertEquals(Long.valueOf(4), count.invert(7L, 3L));
        // removing a single tuple
        Assert.assertEquals(Long.valueOf(6), count.liftAndInvert(7L, "a"));
    }
}
