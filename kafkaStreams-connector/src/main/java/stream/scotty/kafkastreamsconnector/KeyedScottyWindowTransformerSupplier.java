package stream.scotty.kafkastreamsconnector;

import stream.scotty.core.windowFunction.AggregateFunction;
import stream.scotty.core.windowType.SlidingWindow;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.Window;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;

public class KeyedScottyWindowTransformerSupplier<Key, Value, Result> implements TransformerSupplier<Key, Value, Result> {

  private final AggregateFunction<Value, ?, ?> windowFunction;
  private final long allowedLateness;
  private final List<Window> windows;

  public KeyedScottyWindowTransformerSupplier(AggregateFunction<Value, ?, ?> windowFunction, long allowedLateness) {
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
  public KeyedScottyWindowTransformerSupplier<Key, Value, Result> addWindow(Window window) {
      windows.add(window);
      return this;
  }
  
  @Override
  public Transformer<Key, Value, Result> get() {
    final KeyedScottyWindowTransformer<Key, Value, Result> processor =
        new KeyedScottyWindowTransformer<>(this.windowFunction, this.allowedLateness);

    for (Window window : this.windows) {
        processor.addWindow(window);
    }

    return processor;
  }

}
