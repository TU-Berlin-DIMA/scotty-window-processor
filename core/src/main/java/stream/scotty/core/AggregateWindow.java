package stream.scotty.core;

import stream.scotty.core.windowType.*;

import java.io.*;
import java.util.List;

public interface AggregateWindow<T> extends Serializable {


    public WindowMeasure getMeasure();

    public long getStart();

    public long getEnd();

    public List<T> getAggValues();

    public boolean hasValue();

}
