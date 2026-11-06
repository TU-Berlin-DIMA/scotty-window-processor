package stream.scotty.core;

import stream.scotty.core.windowType.*;

public interface WindowCollector {

    public void trigger(long start, long end, WindowMeasure measure);
}
