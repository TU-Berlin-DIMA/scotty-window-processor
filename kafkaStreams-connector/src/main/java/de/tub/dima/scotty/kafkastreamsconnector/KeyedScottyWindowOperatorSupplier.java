package de.tub.dima.scotty.kafkastreamsconnector;

import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KeyedScottyWindowOperatorSupplier<Key, Value> implements ProcessorSupplier<Key, Value> {

  private final AggregateFunction<Value, ?, ?> windowFunction;
  private final long allowedLateness;
  private final List<Window> windows;

  public KeyedScottyWindowOperatorSupplier(AggregateFunction<Value, ?, ?> windowFunction, long allowedLateness) {
      this.windowFunction = windowFunction;
      this.windows = new ArrayList<>();
      this.allowedLateness = allowedLateness;
  }

  /**
   * Register a new @{@link Window} definition that should be added to the Window Operator.
   * For example {@link SlidingWindow} or {@link TumblingWindow}
   *
   * @param window the new window definition
   */
  public KeyedScottyWindowOperatorSupplier<Key, Value> addWindow(Window window) {
      windows.add(window);
      return this;
  }
  
  @Override
  public Processor<Key, Value> get() {
    final KeyedScottyWindowOperator<Key, Value> processor =
        new KeyedScottyWindowOperator<>(this.windowFunction, this.allowedLateness);

    for (Window window : this.windows) {
        processor.addWindow(window);
    }

    return processor;
  }

}
