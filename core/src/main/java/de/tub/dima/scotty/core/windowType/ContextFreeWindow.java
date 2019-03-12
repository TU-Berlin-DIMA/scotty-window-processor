package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.*;

public interface ContextFreeWindow extends Window {

    //Retrieve next window edge for on-the-fly stream slicing
    long assignNextWindowStart(long position);
    /*
    Triggers the final window aggregation according to a watermark
    WindowManager calls this function when it processes a Watermark.
    Method reports all windows which ended between lastWatermark and currentWatermark
    */
    void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark);
}
