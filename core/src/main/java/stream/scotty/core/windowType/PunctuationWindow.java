package stream.scotty.core.windowType;

import stream.scotty.core.WindowCollector;
import stream.scotty.core.windowType.windowContext.WindowContext;
import org.apache.flink.api.java.tuple.Tuple;

public class PunctuationWindow implements ForwardContextFree {

    private final WindowMeasure measure;

    private Object punctuation;

    /**
     * the measure of the Punctuation Window is time
     * @param punctuation defines how the punctuation tuple looks like
     * stream.scotty.slicing.aggregationstore.test.windowTest.PunctuationWindowTest
     * and stream.scotty.slicing.aggregationstore.test.windowTest.PunctuationWindowTupleTest
     * for examples for punctuations
     */
    public PunctuationWindow(Object punctuation) {
        this.measure = WindowMeasure.Time;
        this.punctuation = punctuation;
    }

    @Override
    public WindowMeasure getWindowMeasure() {
        return measure;
    }

    public PunctuationWindow.PunctuationContext createContext() {
        return new PunctuationWindow.PunctuationContext();
    }

    public class PunctuationContext extends WindowContext<Object> implements Comparable {

        private long next_start = 0; //contains punctuation timestamp i.e. where the next window starts
        private long lastWindowEnd = 0; //contains last punctuation to avoid that current windows are triggered
        private boolean isPunctuation = false;

        @Override
        public int compareTo(Object tuple) {
            // compares the processed tuple to the defined punctuation object
            if(punctuation instanceof Tuple && tuple instanceof Tuple) {
                int matchingFields = 0;
                int fields = ((Tuple) punctuation).getArity();
                for (int i = 0; i < fields; i++) {
                    if(((Tuple) punctuation).getField(i) instanceof String){ //attribute in field i is of type String

                        String punctuationString = ((Tuple) punctuation).getField(i).toString();
                        String tupleString = ((Tuple) tuple).getField(i).toString();
                        if(tupleString.matches(punctuationString)){ //matches regular expressions
                            matchingFields++;
                        }
                    }else{
                        if(((Tuple) tuple).getField(i).equals(((Tuple) punctuation).getField(i))){
                            matchingFields++;
                        }
                    }
                }
                return matchingFields == fields ? 0 : -1;
            }else{ //simple check if value is just a integer, double etc.
                if(tuple.equals(punctuation)) return 0;
            }
            return -1;
        }

        public void processPunctuation(Object tuple, long timestamp){
            //assigns timestamp of punctuation to variable
            if(compareTo(tuple) == 0){
                this.next_start = timestamp;
                isPunctuation = true;
            }
        }

        @Override
        public ActiveWindow updateContext(Object tuple, long position) {
            if((position == next_start) && isPunctuation){
                isPunctuation = false;
                int wIndex = getPunctuationWindow(position);
                if (wIndex == -1){
                    addNewWindow(0, position, position);
                    return getWindow(0);
                }else{
                    ActiveWindow lastWindow = getWindow(wIndex);
                    if (lastWindow.getEnd() <= next_start) { //punctuation in-order, add new window
                        lastWindowEnd = next_start;
                        shiftEnd(lastWindow, next_start);
                        return addNewWindow(wIndex + 1, next_start, position);
                    }else{ //out of order punctuation: split window
                        long last_ts = lastWindow.getEnd();
                        shiftEnd(lastWindow, next_start);
                        return addNewWindow(wIndex+1,next_start, last_ts);
                    }
                }
            }else { //normal tuple
                if (hasActiveWindows()) {
                    addNewWindow(0, position, position);
                    return getWindow(0);
                }

                int wIndex = getPunctuationWindow(position);
                if (wIndex == -1) {
                    addNewWindow(0, position, position);
                } else {
                    ActiveWindow lastWindow = getWindow(wIndex);
                    if (lastWindow.getEnd() <= position) { // append to active window
                        shiftEnd(lastWindow, position);
                        return lastWindow;
                    } else { //out-of-order tuple, just insert into corresponding slice
                        return lastWindow;
                    }
                }
            }
            return null;
        }

        @Override
        public long assignNextWindowStart(long position) {
            if(next_start <= position){ //do not append new Slice
                return Long.MAX_VALUE;
            }else{ //flex_count = 1 -> append new Slice
                return this.next_start;
            }
        }

        public int getPunctuationWindow(long position) {
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
                "punctuation" + punctuation.toString()+
                '}';
    }
}
