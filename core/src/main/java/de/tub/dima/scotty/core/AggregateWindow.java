package de.tub.dima.scotty.core;

import de.tub.dima.scotty.core.windowType.*;

import java.io.*;
import java.util.List;

public interface AggregateWindow<T> extends Serializable {


    public WindowMeasure getMeasure();

    public long getStart();

    public long getEnd();

    public List<T> getAggValues();

}
