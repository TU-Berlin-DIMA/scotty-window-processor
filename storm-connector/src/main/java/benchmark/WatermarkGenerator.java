package benchmark;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

//Expects spout/bolt instance that sends tuples
public class WatermarkGenerator extends BaseBasicBolt {

    //private final long maxOutOfOrderness = seconds(20).toMilliseconds(); // 5 seconds
    //private long currentMaxTimestamp;
    //private long startTime = System.currentTimeMillis();
    private long lastWatermark = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    @Override
    //Called everytime a tuple arrives to the bolt
    public void execute(Tuple input, BasicOutputCollector collector) {
        //Emit the incoming tuple directly to the next Bolt
        collector.emit(new Values(input.getValue(0), input.getString(1), input.getValue(2), input.getLong(3)));

        //Emit Watermarks every 1 sec
        long currentTime = System.currentTimeMillis();
        if (currentTime > lastWatermark + 1000 ) {
            //Key, Value or TimeStamp fields are not used. What matters is only catching a watermark in the following Bolt instance.
            //Becareful about the timestamp of the watermark, which will be used to output expired slices from the aggregate store

            //Event time watermarks
            collector.emit(new Values("waterMark", input.getString(1), 1,  input.getLong(3)));

            //Processing time watermarks
            //collector.emit(new Values("waterMark", input.getString(1), 1, currentTime));
            lastWatermark = currentTime;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type", "key", "value", "ts"));
    }
}
