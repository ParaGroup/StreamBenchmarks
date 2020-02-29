package LinearRoad;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.PositionReport;
import util.Log;

public class Dispatcher extends RichFlatMapFunction<Tuple2<Long, String>, Tuple2<Long, PositionReport>> {
    private static final Logger LOG = Log.get(Dispatcher.class);

    @Override
    public void flatMap(Tuple2<Long, String> tuple, Collector<Tuple2<Long, PositionReport>> collector) throws Exception {
        // fetch values from the tuple
        long timestamp = tuple.f0;
        String line = tuple.f1;
        LOG.debug("Dispatcher: {}", line);

        // parse the string
        String[] token = line.split(",");
        short type = Short.parseShort(token[0]);
        if (type == 0) { // TODO constant
            // build and emit the position report
            PositionReport positionReport = new PositionReport();
            positionReport.type = type;
            positionReport.time = Integer.parseInt(token[1]);
            positionReport.vid = Integer.parseInt(token[2]);
            positionReport.speed = Integer.parseInt(token[3]);
            positionReport.xway = Integer.parseInt(token[4]);
            positionReport.lane = Short.parseShort(token[5]);
            positionReport.direction = Short.parseShort(token[6]);
            positionReport.segment = Short.parseShort(token[7]);
            positionReport.position = Integer.parseInt(token[8]);
            collector.collect(new Tuple2<>(timestamp, positionReport));
        }
    }
}
