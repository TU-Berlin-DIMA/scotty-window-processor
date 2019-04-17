package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.*;

public interface ContextFreeWindow extends Window {

    long assignNextWindowStart(long position);

    void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark);

    long clearDelay();
}
