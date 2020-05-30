package de.tub.dima.scotty.core.windowType;

import de.tub.dima.scotty.core.WindowCollector;
import de.tub.dima.scotty.core.windowType.windowContext.WindowContext;
import org.apache.flink.api.java.tuple.Tuple;

public class PunctuationWindow implements ForwardContextFree {

    private final WindowMeasure measure;

    private Object p;

    /**
     * the measure of the Punctuation Window is time
     * @param p defines how the punctuation tuple looks like
     */

    public PunctuationWindow(Object p) {
        this.measure = WindowMeasure.Time;
        this.p = p;
    }

    @Override
    public WindowMeasure getWindowMeasure() {
        return measure;
    }

    public PunctuationWindow.PunctuationContext createContext() {
        return new PunctuationWindow.PunctuationContext();
    }

    public class PunctuationContext extends WindowContext<Object> implements Comparable {

        private long next_start = 0; //contains punctuation timestamp
        private long lastWindowEnd = 0; //contains last punctuation to avoid that current windows are triggered
        private boolean isPunctuation = false;

        @Override
        public int compareTo(Object o) {
            // compares the processed tuple to the defined punctuation object
            if(p instanceof Tuple && o instanceof Tuple) {
                int match = ((Tuple) p).getArity();
                int fields = match;
                for (int i = 0; i < fields; i++) {
                    if(((Tuple) p).getField(i) instanceof String){ //attribute in field i is of type String

                        String s = ((Tuple) p).getField(i).toString();
                        String m = ((Tuple) o).getField(i).toString();
                        if(m.matches(s)){ //matches regular expressions
                            match--;
                        }
                    }else{
                        if(((Tuple) o).getField(i).equals(((Tuple) p).getField(i))){
                            match--;
                        }
                    }
                }
                return match == 0 ? match : -1;
            }else{ //simple check if value is just a integer, double etc.
                if(o.equals(p)) return 0;
            }
            return -1;
        }

        public void processPunctuation(Object o, long punctuation){
            //assigns timestamp of punctuation to variable
            if(compareTo(o) == 0){
                this.next_start = punctuation;
                isPunctuation = true;
            }
        }

        @Override
        public ActiveWindow updateContext(Object o, long position) {
            if((position == next_start) && isPunctuation){ //tuple is punctuation compareTo(o) == 0
                isPunctuation = false;
                int wIndex = getPWindow(position);
                if (wIndex == -1){
                    addNewWindow(0, position, position);
                    return getWindow(0);
                }else{
                    ActiveWindow p = getWindow(wIndex);
                    if (p.getEnd() <= next_start) { //punctuation in-order, add new window
                        lastWindowEnd = next_start;
                        shiftEnd(p, next_start);
                        return addNewWindow(wIndex + 1, next_start, position);
                    }else{ //out of order punctuation: split windows
                        long last_ts = p.getEnd();
                        shiftEnd(p, next_start);
                        return addNewWindow(wIndex+1,next_start, last_ts);
                    }
                }
            }else { //normal tuple
                if (hasActiveWindows()) {
                    addNewWindow(0, position, position);
                    return getWindow(0);
                }

                int wIndex = getPWindow(position);
                if (wIndex == -1) {
                    addNewWindow(0, position, position);
                } else {
                    ActiveWindow p = getWindow(wIndex);
                    if (p.getEnd() <= position) { // append to active window
                        shiftEnd(p, position);
                        return p;
                    } else { //out-of-order tuple, just insert into corresponding slice
                        return p;
                    }
                }
            }
            return null;
        }

        /*@Override
        public ActiveWindow updateContextWindows(Object element, long ts, ArrayList<Long> listOfTs) {
            return null;
        }*/

        @Override
        public long assignNextWindowStart(long position) {
            if(next_start <= position){ //do not append new Slice
                return Long.MAX_VALUE;
            }else{ //flex_count = 1 -> append new Slice
                return this.next_start;
            }
        }

        public int getPWindow(long position) {
            // returns latest window to which the tuple belongs
            int i = numberOfActiveWindows()-1;
            for (; i >= 0 ; i--) {
                ActiveWindow p = getWindow(i);
                if (p.getStart() <= position) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public void triggerWindows(WindowCollector aggregateWindows, long lastWatermark, long currentWatermark) {
            ActiveWindow window = getWindow(0);
            while (window.getEnd() <= currentWatermark && window.getEnd() <= lastWindowEnd && window.getStart() != window.getEnd()) {
                aggregateWindows.trigger(window.getStart(), window.getEnd(), measure);
                removeWindow(0);
                if (hasActiveWindows())
                    return;
                window = getWindow(0);
            }
        }
    }

    @Override
    public String toString() {
        return "PunctuationWindow{" +
                "measure=" + measure +
                "punctuation" + p.toString()+
                '}';
    }
}
