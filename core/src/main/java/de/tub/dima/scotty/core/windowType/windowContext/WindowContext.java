package de.tub.dima.scotty.core.windowType.windowContext;

import de.tub.dima.scotty.core.*;
import de.tub.dima.scotty.core.*;

import java.io.*;
import java.util.*;

public abstract class WindowContext<Tuple> implements Serializable {

    private Set<WindowModifications> modifiedWindowEdges;

    private final ArrayList<ActiveWindow> activeWindows = new ArrayList<>();

    public final boolean hasActiveWindows() {
        return activeWindows.isEmpty();
    }

    public final ActiveWindow addNewWindow(int i, long start, long end){
        ActiveWindow newWindow = new ActiveWindow(start, end);
        activeWindows.add(i, newWindow);
        modifiedWindowEdges.add(new AddModification(start));
        modifiedWindowEdges.add(new AddModification(end));
        return newWindow;
    }

    public ArrayList<ActiveWindow> getActiveWindows() {
        return activeWindows;
    }

    public final ActiveWindow getWindow(int i) {
        return activeWindows.get(i);
    }

    public final int numberOfActiveWindows() {
        return activeWindows.size();
    }

    public final ActiveWindow mergeWithPre(int sessionIndex) {
        assert sessionIndex >= 1;
        ActiveWindow window = activeWindows.get(sessionIndex);
        ActiveWindow preWindow = activeWindows.get(sessionIndex - 1);
        shiftEnd(preWindow, window.getEnd());
        removeWindow(sessionIndex);
        return preWindow;
    }

    public final void removeWindow(int index) {
        modifiedWindowEdges.add(new DeleteModification(activeWindows.get(index).start));
        modifiedWindowEdges.add(new DeleteModification(activeWindows.get(index).end));
        activeWindows.remove(index);
    }


    public void shiftStart(ActiveWindow window, long position) {
        modifiedWindowEdges.add(new ShiftModification(window.start, position));
        window.setStart(position);
    }

    public void shiftStartDontModify(ActiveWindow window, long position) { // does not modify the start edge
        //modifiedWindowEdges.add(new ShiftModification(window.start, position));
        window.setStart(position);
    }

    public void shiftEnd(ActiveWindow window, long position) {
        //modifiedWindowEdges.add(new ShiftModification(window.end, position));
        window.setEnd(position);
    }


    public abstract ActiveWindow updateContext(Tuple tuple, long position);

    public final ActiveWindow updateContext(Tuple tuple, long position, Set<WindowModifications> modifiedWindowEdges){
        this.modifiedWindowEdges = modifiedWindowEdges;
        return updateContext(tuple, position);
    };

    public abstract ActiveWindow updateContextWindows(Tuple element, long ts, ArrayList<Long> listOfTs); //For out-of-order processing

    public ActiveWindow updateContextWindows(Tuple element, long ts, ArrayList<Long> listOfTs, Set<WindowModifications> windowModifications) {
        this.modifiedWindowEdges = windowModifications;
        return updateContextWindows(element, ts, listOfTs);
    }
    public abstract long assignNextWindowStart(long position);

    public abstract void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark);

    public class ActiveWindow implements Comparable<ActiveWindow> {
        private long start;
        private long end;

        private ActiveWindow(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public final long getEnd() {
            return end;
        }

        public final long getStart() {
            return start;
        }

        private final void setEnd(long end) {
            this.end = end;
        }

        private final void setStart(long start) {
            this.start = start;
        }

        @Override
        public int compareTo(ActiveWindow o) {
            return Long.compare(this.start, o.start);
        }
    }
}
