package stream.scotty.core.windowType;

import stream.scotty.core.*;

import java.io.*;

public interface Window extends Serializable {
    WindowMeasure getWindowMeasure();
}
