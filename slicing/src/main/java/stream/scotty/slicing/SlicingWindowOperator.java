/*
 * Copyright The Scotty Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package stream.scotty.slicing;

import stream.scotty.core.AggregateWindow;
import stream.scotty.core.WindowOperator;
import stream.scotty.core.windowFunction.AggregateFunction;
import stream.scotty.core.windowType.PunctuationWindow;
import stream.scotty.core.windowType.Window;
import stream.scotty.core.windowType.windowContext.WindowContext;
import stream.scotty.slicing.aggregationstore.AggregationStore;
import stream.scotty.slicing.aggregationstore.LazyAggregateStore;
import stream.scotty.slicing.slice.SliceFactory;
import stream.scotty.state.StateFactory;

import java.util.List;


/**
 * Implementation of the slicing window operator based on the Scotty technique:
 * Scotty: Efficient AbstractWindow Aggregation for out-of-order Stream Processing:
 * Jonas Traub, Philipp M. Grulich, Alejandro Rodrıguez Cuéllar, Sebastian Breß, Asterios Katsifodimos, Tilmann Rabl, Volker Markl *
 * @param <InputType>
 */
public class SlicingWindowOperator<InputType> implements WindowOperator<InputType> {

    private final StateFactory stateFactory;

    private final WindowManager windowManager;
    private final SliceFactory<Integer,Integer> sliceFactory;
    private final SliceManager<InputType> sliceManager;
    private final StreamSlicer slicer;

    public SlicingWindowOperator(StateFactory stateFactory) {
        AggregationStore<InputType> aggregationStore = new LazyAggregateStore<>();
        this.stateFactory = stateFactory;
        this.windowManager = new WindowManager(stateFactory, aggregationStore);
        this.sliceFactory = new SliceFactory<>(windowManager, stateFactory);
        this.sliceManager = new SliceManager<>(sliceFactory, aggregationStore, windowManager);
        this.slicer = new StreamSlicer(sliceManager, windowManager);
    }

    @Override
    public void processElement(InputType element, long ts) {
        /*added for Punctuation Window*/
        for (WindowContext windowContext : this.windowManager.getContextAwareWindows()) {
            if(windowContext instanceof PunctuationWindow.PunctuationContext){
                ((PunctuationWindow.PunctuationContext)windowContext).processPunctuation(element, ts);
            }
        }
        slicer.determineSlices(ts);
        sliceManager.processElement(element, ts);
    }

    @Override
    public List<AggregateWindow> processWatermark(long watermarkTs) {
        return windowManager.processWatermark(watermarkTs);
    }

    /**
     * @return true if a future watermark can still trigger windows that contain tuples, without new tuples arriving.
     */
    public boolean hasPendingWindows() {
        return windowManager.hasPendingWindows();
    }

    @Override
    public void addWindowAssigner(Window window) {
        windowManager.addWindowAssigner(window);
    }

    @Override
    public <OutputType> void addAggregation(AggregateFunction<InputType, ?, OutputType> windowFunction) {
        windowManager.addAggregation(windowFunction);
    }

    public <Agg, OutputType> void addWindowFunction(AggregateFunction<InputType, Agg, OutputType> windowFunction) {
        windowManager.addAggregation(windowFunction);
    }

    @Override
    public void setMaxLateness(long maxLateness) {
        windowManager.setMaxLateness(maxLateness);
    }

    public void setResendWindowsInAllowedLateness(boolean resendWindowsInAllowedLateness) {this.windowManager.setResendWindowsInAllowedLateness(resendWindowsInAllowedLateness);}
}
