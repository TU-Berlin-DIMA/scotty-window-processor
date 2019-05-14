package de.tub.dima.scotty.core;

import de.tub.dima.scotty.core.windowType.*;

public interface WindowCollector {

    public void trigger(long start, long end, WindowMeasure measure);
}
