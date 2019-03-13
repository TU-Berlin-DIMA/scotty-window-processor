package de.tub.dima.scotty.slicing.slice;


import de.tub.dima.scotty.slicing.state.AggregateState;
import de.tub.dima.scotty.slicing.WindowManager;
import de.tub.dima.scotty.state.StateFactory;

public class EagerSlice<InputType, ValueType> extends AbstractSlice<InputType, ValueType> {

    private final AggregateState<InputType> state;

    public EagerSlice(StateFactory stateFactory, WindowManager windowManager, long startTs, long endTs, Type type) {
        super(startTs, endTs, type);
        this.state = new AggregateState<InputType>(stateFactory, windowManager.getAggregations());
    }

    @Override
    public AggregateState getAggState() {
        return state;
    }

    @Override
    public void addElement(InputType element, long ts) {
        super.addElement(element, ts);
        state.addElement(element);
    }


}
