package de.tub.dima.scotty.slicing.state;

import de.tub.dima.scotty.state.StateFactory;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;

import java.util.List;
import java.util.Objects;

public class AggregateWindowState implements AggregateWindow {

    private final long startTs;
    private final long endTs;

    private AggregateState windowState;

    public AggregateWindowState(long startTs, long endTs, StateFactory stateFactory, List<AggregateFunction> windowFunctionList) {
        this.startTs = startTs;
        this.endTs = endTs;
        this.windowState = new AggregateState(stateFactory, windowFunctionList);
    }

    public long getStartTs() {
        return startTs;
    }

    public long getEndTs() {
        return endTs;
    }

    @Override
    public List getAggValue() {
        return windowState.getValues();
    }

    public void addState(AggregateState aggregationState) {
        this.windowState.merge(aggregationState);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateWindowState that = (AggregateWindowState) o;
        return startTs == that.startTs &&
                endTs == that.endTs &&
                Objects.equals(windowState, that.windowState);
    }

    @Override
    public int hashCode() {

        return Objects.hash(startTs, endTs, windowState);
    }

    @Override
    public String toString() {
        return "AggregateWindowState{" +
                "startTs=" + startTs +
                ", endTs=" + endTs +
                ", windowState=" + windowState +
                '}';
    }
}
