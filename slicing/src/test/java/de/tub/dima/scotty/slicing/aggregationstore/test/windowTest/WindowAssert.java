package de.tub.dima.scotty.slicing.aggregationstore.test.windowTest;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.slicing.*;
import de.tub.dima.scotty.core.*;
import org.junit.*;

import java.util.*;

public class WindowAssert {

    public static void assertEquals(AggregateWindow aggregateWindow, long start, long end, Object value){
        Assert.assertEquals(start, aggregateWindow.getStartTs());
        Assert.assertEquals(end, aggregateWindow.getEndTs());
        Assert.assertEquals(value, aggregateWindow.getAggValue().get(0));
    }

    public static void assertContains(List<AggregateWindow> resultWindows, int start, int end, Object value) {
        for (AggregateWindow window: resultWindows){
            if(window.getStartTs() == start && window.getEndTs() == end && window.getAggValue().get(0).equals(value)) {
                Assert.assertTrue(true);
                return;
            }
        }
        Assert.assertTrue(false);
    }
}
