package stream.scotty.core.windowType;

import stream.scotty.core.*;
import stream.scotty.core.*;

public interface ContextFreeWindow extends Window {

    long assignNextWindowStart(long position);

    void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark);

    long clearDelay();
}
