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
package stream.scotty.slicing.aggregationstore.test.windowTest;

import stream.scotty.core.*;
import org.junit.*;

import java.util.*;

public class WindowAssert {

    public static void assertEquals(AggregateWindow aggregateWindow, long start, long end, Object value){
        Assert.assertEquals(start, aggregateWindow.getStart());
        Assert.assertEquals(end, aggregateWindow.getEnd());
        Assert.assertEquals(value, aggregateWindow.getAggValues().get(0));
    }

    public static void assertContains(List<AggregateWindow> resultWindows, int start, int end, Object value) {
        for (AggregateWindow window: resultWindows){
            if(window.getStart() == start && window.getEnd() == end && window.getAggValues().get(0).equals(value)) {
                Assert.assertTrue(true);
                return;
            }
        }
        Assert.assertTrue(false);
    }
}
