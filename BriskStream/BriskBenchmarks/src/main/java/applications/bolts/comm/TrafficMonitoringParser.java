package applications.bolts.comm;

import parser.TaxiTraceParser;
import helper.parser.Parser;
import util.Configuration;
import brisk.components.operators.base.MapBolt;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tony on 5/5/2017.
 */
public class TrafficMonitoringParser extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoringParser.class);
    private static final long serialVersionUID = 2921418272215488770L;
    final TaxiTraceParser parser;
    private final Fields fields;

    public TrafficMonitoringParser(Parser parser, Fields fields) {
        super(LOG);
        this.parser = (TaxiTraceParser) parser;
        this.fields = fields;
    }

    public Integer default_scale(Configuration conf) {

        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 6;
        } else {
            return 2;
        }
    }

    @Override
    public Fields getDefaultFields() {
        return fields;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        //not in use
    }

//	volatile Object[] items;
//	Object[] items;

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            char[] string = in.getCharArray(0, i);
            Object[] emit = parser.parse(string);
            Object[] emit2 = new Object[emit.length+1];
            for (int j=0; j<emit.length; j++)
                emit2[j] = emit[j];
            emit2[emit.length] = in.getLong(1, i);     
            System.out.println("Parser manda tupla con " + emit2.length + " campi");
            collector.emit(bid, emit2);
        }
//		//LOG.DEBUG("Parser(" + this.getContext().getThisTaskId() + ") emit:" + bid);
//		this.collector.try_fill_gap();
    }
}
