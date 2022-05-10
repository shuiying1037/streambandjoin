package soj.biclique.bolt;

import java.awt.*;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import static com.google.common.collect.Lists.newArrayList;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.Constants;
import soj.util.FileWriter;
import soj.util.GeoHash;
import soj.util.Stopwatch;

import static java.util.concurrent.TimeUnit.*;
import static soj.util.CastUtils.getList;

//import org.davidmoten.hilbert.*;
import soj.util.hilbert;


public class
ShuffleBolt extends BaseRichBolt
{
    private OutputCollector _collector;
    private static final List<String> SCHEMA = ImmutableList.of("type", "relation", "timestamp", "key", "value");
    private FileWriter _output;
    private long _r;
    private long _s;
    private long _lastTime;
    private Stopwatch _stopwatch;
    private String _rStream;
    private String _sStream;
    private hilbert myhil;
    private int _bits;
    private long minCols1, minCols2;
    private long mask;

    private static final org.davidmoten.hilbert.HilbertCurve c = org.davidmoten.hilbert.HilbertCurve.bits(5).dimensions(2);

//    private static org.davidmoten.hilbert.SmallHilbertCurve small = org.davidmoten.hilbert.HilbertCurve.small().bits(5).dimensions(2);

/*    @Test
    public void testIndex1() {
        org.davidmoten.hilbert.HilbertCurve c = org.davidmoten.hilbert.HilbertCurve.bits(2).dimensions(2);
        assertEquals(7, c.index(1, 2).intValue());
    }*/

    public ShuffleBolt(String datasize){
//        _rStream = "didiOrder" + datasize;
//        _sStream = "didiGps" + datasize;
        _rStream = "Orders2";
        _sStream = "Gps2";
    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        _output = new FileWriter("/home/shuiyinyu/join/tmpresult-fastjoin", "zsj_shuffle" + topologyContext.getThisTaskId(), "txt");
        _r = 0;
        _s = 0;
        _lastTime = 0;
        _stopwatch = Stopwatch.createStarted();
        _bits = 13;
        myhil = new hilbert();
        minCols1 = 10404000L;
        minCols2 = 3065200L;
        mask = 1<<_bits;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SCHEMA));
    }
    @Override
    public void execute(Tuple tuple) {
        String topic = tuple.getStringByField("topic");
        String value = tuple.getStringByField("value");
        String rel;
        Long ts = System.currentTimeMillis();
        Long key = 0L;
        value = value.trim();
//        output("value=" +  Long.parseLong(value) + "\n");
        ///zipf分布数据，由于数据集存在负数。要把负数改成unsigned。先取绝对值/比特位。
/*       long keyl = (long) Long.parseLong(value);
        if(keyl<0){
            Long unsignedKey = keyl & Integer.MAX_VALUE;
            unsignedKey |= 0x80000000L;
            key = unsignedKey;
        }
        else key = keyl;*/
//        output("unsigned key:" + key);
        //TODO:stock纳斯达克数据集的值域范围：3~423401

/*        String []cols = value.split("\t");
        String skey = cols[2];
        skey = skey.replace(",","");
        key = (long) (Double.parseDouble(skey)*100);*/
//        output("key=" + key + "\n");
        ///TODO:这个是滴滴的数据
        String []cols = value.split(",");
        key = Long.parseLong(cols[1]);
//        key = (long) (Double.parseDouble(cols[1]))*(1<<13);
        long cols1 = (long) ((Double.parseDouble(cols[1])-104042000)/10);//%(2<<17)
        long cols2 = (long) ((Double.parseDouble(cols[2])-30652900)/10);//%(2<<15)
        hilbert.PointH pointh = myhil.new PointH(cols1,cols2);
        key = myhil.xy2d(mask, pointh);
        key = (long) (key)/(1<<_bits);
//        key = (long) Math.floor((key-minrange)/(1<<_bits));

//        output("topic="+topic+",cols[1]="+cols[1]+",cols[2]="+cols[2]+",cols1="+cols1+",cols2="+cols2+",key="+key);
//        output("key="+key);
        if(topic.equals(_rStream)) {
            rel = "R";
            _r ++;
        } else if(topic.equals(_sStream)) {
            rel = "S";
            _s++;
        } else {
            rel = "false";
        }
//        long selftime = (long)(Double.parseDouble(cols[0]));
        _collector.emit(new Values("tuple", rel, ts, key, value));
    }

    private void output(String msg) {
        if (_output != null){
            //_output.write(msg);
            _output.writeImmediately(msg);
        }
    }

    @Override
    public void cleanup() {
        if (_output != null) {
            _output.endOfFile();
        }
    }

/*    private boolean isTimeToOutputProfile() {
        long currTime = _stopwatch.elapsed(SECONDS);

        if (currTime >= _triggerReportInSeconds) {
            _triggerReportInSeconds = currTime + _profileReportInSeconds;
            return true;
        }
        else {
            return false;
        }
    }*/

}