package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.windowType.windowContext.*;

public class SessionWindow implements ForwardContextAware {

    private final WindowMeasure measure;
    /**
     * The session window gap
     */
    private final long gap;

    /**
     * @param measure WindowMeasurement
     * @param gap     Session Gap
     */
    public SessionWindow(WindowMeasure measure, long gap) {
        this.measure = measure;
        this.gap = gap;
    }

    public long getGap() {
        return gap;
    }

    @Override
    public WindowMeasure getWindowMeasure() {
        return measure;
    }

    @Override
    public SessionContext createContext() {
        return new SessionContext();
    }

    public class SessionContext extends WindowContext<Object> {

        @Override
        public ActiveWindow updateContext(Object tuple, long position) {

            if (hasActiveWindows()) {
                addNewWindow(0, position, position);
                return getWindow(0);
            }
            int sessionIndex = getSession(position);

            if (sessionIndex == -1) {
                addNewWindow(0, position, position);

            } else {
                ActiveWindow s = getWindow(sessionIndex);
                if (s.getStart() - gap > position) {
                    // add new session before
                    return addNewWindow(sessionIndex, position,position);
                } else if (s.getStart() > position && s.getStart() - gap < position) {
                    // expand start
                    shiftStart(s, position);
                    if (sessionIndex > 0) {
                        // merge with pre
                        ActiveWindow preSession = getWindow(sessionIndex - 1);
                        if (preSession.getEnd() + gap >= s.getStart()) {
                            return this.mergeWithPre(sessionIndex);
                        }
                    }
                    return s;
                } else if (s.getEnd() < position && s.getEnd() + gap >= position) {
                    shiftEnd(s, position);
                    if (sessionIndex < numberOfActiveWindows() - 1) {
                        ActiveWindow nextSession = getWindow(sessionIndex + 1);
                        // merge with next Session
                        if (s.getEnd() + gap >= nextSession.getStart()) {
                            return this.mergeWithPre(sessionIndex + 1);
                        }
                    }
                    return s;

                } else if (s.getEnd() + gap < position) {
                    // add new session after
                    return addNewWindow(sessionIndex + 1, position,position);
                }
            }
            return null;
        }

        public int getSession(long position) {

            int i = 0;
            for (; i < numberOfActiveWindows(); i++) {
                ActiveWindow s = getWindow(i);
                if (s.getStart() - gap <= position && s.getEnd() + gap >= position) {
                    return i;
                } else if (s.getStart() - gap > position)
                    return i - 1;

            }
            return i - 1;
        }


        @Override
        public long assignNextWindowStart(long position) {
            return position + gap;
        }

        @Override
        public void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark) {
            ActiveWindow session = getWindow(0);
            while (session.getEnd() + gap < currentWatermark) {
                aggregateWindows.trigger(session.getStart(), session.getEnd() + gap, measure);
                removeWindow(0);
                if (hasActiveWindows())
                    return;
                session = getWindow(0);
            }
        }


    }

    @Override
    public String toString() {
        return "SessionWindow{" +
                "measure=" + measure +
                ", gap=" + gap +
                '}';
    }
}
