package de.tub.dima.scotty.core;

import de.tub.dima.scotty.core.windowFunction.*;

public interface WindowCollector {

    //Trigger the computation and output of the window aggregate
    public void trigger(long start, long end);
}
