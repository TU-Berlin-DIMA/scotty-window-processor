package de.tub.dima.scotty.samzaconnector.demo;

import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.samzaconnector.KeyedScottyWindowOperator;
import de.tub.dima.scotty.samzaconnector.demo.windowFunctions.SumWindowFunction;
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
                (new SumWindowFunction(), 100, stream);
        operator.addWindow(new SlidingWindow(WindowMeasure.Time, 5000, 1000));
        operator.addWindow(new TumblingWindow(WindowMeasure.Time, 2000));

        return operator;
    }
}
