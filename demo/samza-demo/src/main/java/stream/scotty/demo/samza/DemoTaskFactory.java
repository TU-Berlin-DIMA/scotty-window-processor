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
package stream.scotty.demo.samza;

import stream.scotty.core.windowType.SlidingWindow;
import stream.scotty.core.windowType.TumblingWindow;
import stream.scotty.core.windowType.WindowMeasure;
import stream.scotty.samzaconnector.KeyedScottyWindowOperator;
import stream.scotty.core.windowFunction.builtin.Sum;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;

public class DemoTaskFactory implements StreamTaskFactory {
    private String SYSTEM_DESCRIPTOR_NAME;
    private String OUTPUT_DESCRIPTOR_NAME;

    public DemoTaskFactory(String SYSTEM_DESCRIPTOR_NAME, String OUTPUT_DESCRIPTOR_NAME) {
        this.SYSTEM_DESCRIPTOR_NAME = SYSTEM_DESCRIPTOR_NAME;
        this.OUTPUT_DESCRIPTOR_NAME = OUTPUT_DESCRIPTOR_NAME;
    }

    @Override
    public StreamTask createInstance() {
        SystemStream stream = new SystemStream(SYSTEM_DESCRIPTOR_NAME, OUTPUT_DESCRIPTOR_NAME);
        KeyedScottyWindowOperator operator = new KeyedScottyWindowOperator<Integer, Integer>
                (Sum.create(), 100, stream);
        operator.addWindow(new SlidingWindow(WindowMeasure.Time, 5000, 1000));
        operator.addWindow(new TumblingWindow(WindowMeasure.Time, 2000));

        return operator;
    }
}
