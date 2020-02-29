package applications.bolts.lg;

import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import model.geoip.IPLocation;
import model.geoip.IPLocationFactory;
import model.geoip.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.datatypes.StreamValues;

import static constants.BaseConstants.BaseConf;
import static constants.ClickAnalyticsConstants.Field;

/**
 * User: domenicosolazzo
 */
public class GeographyBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GeographyBolt.class);
    private static final long serialVersionUID = 8338380760260959675L;

    private IPLocation resolver;

    private double cnt = 0, cnt1 = 0;

    public GeographyBolt() {
        super(LOG);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        String ipResolver = config.getString(BaseConf.GEOIP_INSTANCE);
        resolver = IPLocationFactory.create(ipResolver, config);
        LOG.info(Thread.currentThread().getName());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//        cnt++;
//        if (stat != null) stat.start_measure();
        final long bid = in.getBID();
        String ip = in.getStringByField(Field.IP);
        Location location = resolver.resolve(ip);

        if (location != null) {
            String city = location.getCity();
            String country = location.getCountryName();
//            cnt1++;


            collector.emit(bid, new StreamValues(country, city));

        }
//        double i=(cnt1-cnt)/cnt;
//        if (stat != null) stat.end_measure();
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {

        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            String ip = in.getStringByField(Field.IP, i);
            Location location = resolver.resolve(ip);

            if (location != null) {
                String city = location.getCity();
                String country = location.getCountryName();

                collector.emit(bid, new StreamValues(country, city));

            }
        }
    }


    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.COUNTRY, Field.CITY);
    }
}
