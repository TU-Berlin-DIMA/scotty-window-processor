package stream.scotty.core.windowType;

import stream.scotty.core.windowType.windowContext.*;
import stream.scotty.core.windowType.windowContext.*;

public interface ForwardContextAware extends Window {

    WindowContext createContext();
}
