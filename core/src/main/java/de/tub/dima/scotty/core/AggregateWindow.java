package de.tub.dima.scotty.core;

import de.tub.dima.scotty.core.windowType.*;

import java.io.*;
import java.util.List;

public interface AggregateWindow<T> extends Serializable {


    public long getStartTs();

    public long getEndTs();

    public List<T> getAggValue();

}
