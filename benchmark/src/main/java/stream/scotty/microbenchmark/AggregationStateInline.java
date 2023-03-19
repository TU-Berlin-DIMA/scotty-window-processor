package stream.scotty.microbenchmark;

import stream.scotty.state.StateFactory;
import stream.scotty.state.ValueState;

import java.util.ArrayList;

public class AggregationStateInline {

    private boolean empty;
    private int value;

    private ArrayList<ValueState<Integer>> stateList = new ArrayList<>();
    private ArrayList<Integer> v = new ArrayList<>();

    public AggregationStateInline(StateFactory stateFactory) {
        stateList.add(stateFactory.createValueState());
        stateList.get(0).set(0);
        v.add(0);
    }

    public void addElement(Integer state) {
        v.set(0,v.get(0));
    }
}
