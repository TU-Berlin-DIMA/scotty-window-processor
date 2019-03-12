package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.windowType.windowContext.*;

public interface ForwardContextAware extends Window {
    WindowContext createContext();
}
