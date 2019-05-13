package de.tub.dima.scotty.slicing.aggregationstore.test.windowTest;

import de.tub.dima.scotty.core.*;
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
