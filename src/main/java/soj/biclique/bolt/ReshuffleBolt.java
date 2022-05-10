package soj.biclique.bolt;

import java.util.*;

import com.google.common.collect.ImmutableList;
import static com.google.common.collect.Lists.newArrayList;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import soj.biclique.Shard.Shard;
import soj.biclique.router.RouterItem;
import soj.biclique.router.RouterTable;
import soj.util.FileWriter;
import soj.util.Stopwatch;

import static java.util.concurrent.TimeUnit.*;
import static soj.biclique.KafkaTopology.*;
import static soj.util.CastUtils.getList;

import java.util.concurrent.TimeUnit;
import java.lang.Math;
//import com.google.common.math.Quantiles;
/*import org.apache.*;
import org.apache.datasketches.memory.Memory;

import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;*/

public class ReshuffleBolt extends BaseRichBolt
{
//    private static final List<String> SCHEMA = ImmutableList.of("relation", "timestamp", "key", "value", "target");
    private static final List<String> SCHEMA = ImmutableList.of("type", "relation", "timestamp", "key", "value", "boltID", "shardID"); ///"tuple", rel, ts, key, value, taskIdShufferR, 0L
    private static final List<String> TO_MONITOR_SCHEMA = ImmutableList.of("type", "relation", "emitTaskID", "emiShardID", "immTaskID", "immShardID", "minRange", "cursor", "maxRange", "numMigrateTuple");
    private static final String _streamShuffleR = SHUFFLE_R_STREAM_ID;
    private static final String _streamShuffleS = SHUFFLE_S_STREAM_ID;
    private static final String _streamBroadcastR = BROADCAST_R_STREAM_ID;
    private static final String _streamBroadcastS = BROADCAST_S_STREAM_ID;
    private static final String _streamMonitorR = MONITOR_R_STREAM_ID;
    private static final String _streamMonitorS = MONITOR_S_STREAM_ID;
    private static final String _streamReshuffleToMonitorR = RESHUFFLER_TO_MONITOR_R_STREAM_ID;
    private static final String _streamReshuffleToMonitorS = RESHUFFLER_TO_MONITOR_S_STREAM_ID;
    private HashFunction h = Hashing.murmur3_128(13);
    private Stopwatch _stopwatch;
    private OutputCollector _collector;
    private FileWriter _output;
    private long _countKey[][];
    private int _countloop;
    private int _intArr;
    private String _type;
    private long _lastOutputTime;
    private long _r;
    private long _s;
    private int _numInstanceR, _numInstanceS;
    private double _interval;
    private long _rangeParR, _rangeParS;
    private long maxrange,minrange,maxrange2,minrange2;
//    private long rangeJB[][]; ////保存每个joinerB的范围
    private int _numJoinerB;
    private long tstemp;
    private int arr;
    private long _bits;
    private long _nextTime;
    private long _tupleRemit, _tupleSemit;
    private Map<Integer, Integer> _staReshR, _staReshR_BrodS, _staReshS, _staReshS_BrodR;
    private int defaultRouterTableSize;
    //List<String> _componentIDs;
    private String _componentIDR;
    private String _componentIDS;
    private RouterTable<Long> _routerTableR;
    private RouterTable<Long> _routerTableS;
    private List<Integer> _joinerRTaskIDs;
    private List<Integer> _joinerSTaskIDs;
    private long _e1, _e2;
    private boolean _initial;
    private int _numInitShard;
    private int _countR, _countS;
    private List<Integer> _keyR;
    private List<Integer> _keyS;
    private List<Integer> _quantileR;
    private List<Integer> _quantileS;

    private long _maxValue,_minValue,_maxValueS,_minValueS;
    private Long _numRealEmitR, _numRealEmitS, _numReceiveR, _numReceiveS;
    private long _ES1R, _ES1S;

    public ReshuffleBolt(String type, int numPartitionR, int numPartitionS, double interval,
                         String componentIDR, String componentIDS, long e1, long e2, int numInitPerTask){
        _type = type;
        _numInstanceR = numPartitionR;
        _numInstanceS = numPartitionS;
        _interval = interval;
        _componentIDR = componentIDR;
        _componentIDS = componentIDS;
        _countloop = 0;
        _bits = 13;
//        _bits = 7;
        minrange = 0L;
        maxrange = 1L<<(_bits*2); //8192，
        arr = 0;
        tstemp = 0L;
        _numJoinerB = 0;
//        rangeJB  = new long[_numJoinerB][2]; ///////还没赋值
        _tupleRemit = 0L;
        _tupleSemit = 0L;
        _staReshR = new HashMap<>(_numInstanceR * 2);
        _staReshS = new HashMap<>(_numInstanceS * 2);
        _staReshR_BrodS = new HashMap<>(_numInstanceR * 2);
        _staReshS_BrodR = new HashMap<>(_numInstanceS * 2);
        defaultRouterTableSize = numPartitionS; ///////////先用S流的划分作为路由表的划分数量。
        _e1 = e1;
        _e2 = e2;
        _numInitShard = numInitPerTask;
        _minValue = Long.MAX_VALUE;
        _maxValueS = 0L;
        _minValueS = Long.MAX_VALUE;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        _output = new FileWriter("/home/shuiyinyu/join/tmpresult-fastjoin", "Reshuffle_" + topologyContext.getThisTaskId(), "txt");
        _stopwatch = Stopwatch.createStarted();
        _lastOutputTime = _stopwatch.elapsed(TimeUnit.MILLISECONDS);
        _r = 0;
        _s = 0;
        _intArr = 200;
        _countKey = new long[100][_intArr+1];
//        _range = _entireRange/_numInstance;
        _rangeParR = (long) (maxrange - minrange)/_numInstanceR;
        _rangeParS = (long) (maxrange - minrange)/_numInstanceS;
/*        for(int i = 0; i<_numJoinerB; i++){ /////////还没有对rangeJB赋值
            rangeJB[i][0] = 0;
            rangeJB[i][1] = 1;
        }*/
        _nextTime = _stopwatch.elapsed(MILLISECONDS)+3*(long)_interval;
        _routerTableR = new RouterTable<Long>();
        _routerTableS = new RouterTable<Long>();
        _initial = false;

        //获取joiner-r和joiner-s的taskID集合
        if(_componentIDR.isEmpty()) _componentIDR = "joiner-r";
        if(_componentIDS.isEmpty()) _componentIDS = "joiner-s";
        _joinerRTaskIDs = topologyContext.getComponentTasks(_componentIDR);
        _joinerSTaskIDs = topologyContext.getComponentTasks(_componentIDS);

//        long[][] rangeArrR = { {0,448}, {448,850}, {850,1380}, {1380,2150}, {2150,3093}, {3093,4210}, {4210,5370}, {5370,6814}, {6814,7400}, {7400,8200} };
//        long[][] rangeArrS = { {0,448}, {448,850}, {850,1380}, {1380,2150}, {2150,3093}, {3093,4210}, {4210,5370}, {5370,6814}, {6814,7400}, {7400,8200} };
        _numInstanceR = _numInstanceR * _numInitShard;
        _numInstanceS = _numInstanceS * _numInitShard;

        long[][] rangeArrR = new long[_numInstanceR][2];
        long[][] rangeArrS = new long[_numInstanceS][2];

//        long stepR =2000000000L/_numInstanceR;
//        long stepS = 2000000000L/_numInstanceS;
//        long tmpR = 2000000000L%_numInstanceR;
//        long tmpS = 2000000000L%_numInstanceS;
        long stepR = 8200/_numInstanceR;
        long stepS = 8200/_numInstanceS;
        long tmpR = 8200%_numInstanceR;
        long tmpS = 8200%_numInstanceS;
        double threthodR = (double)tmpR/_numInstanceR;
        double threthodS = (double)tmpS/_numInstanceS;
        long sum = 0l;

        for(int i = 0; i < _numInstanceR; i ++){
            rangeArrR[i][0] = sum;
            if(i%_numInstanceR <= tmpR)sum += 1;
            rangeArrR[i][1] = stepR + sum;
            sum = rangeArrR[i][1];
        }
        rangeArrR[0][0] = Long.MIN_VALUE + 1;
        rangeArrR[_numInstanceR-1][1] = Long.MAX_VALUE - 1;
        sum = 0l;
        for(int i = 0; i < _numInstanceS; i ++){
            rangeArrS[i][0] = sum;
            if(i%_numInstanceS <= tmpS)sum += 1;
            rangeArrS[i][1] = stepS + sum;
            sum = rangeArrS[i][1];
        }
        rangeArrS[0][0] = Long.MIN_VALUE + 1;
        rangeArrS[_numInstanceR-1][1] = Long.MAX_VALUE - 1;

        _numInstanceR = _numInstanceR / _numInitShard;
        _numInstanceS = _numInstanceS / _numInitShard;
///////////////
/*      ////滴滴的数据
        int[] quantileR = {0, 90, 137, 199, 267, 317, 337, 358, 388, 404, 443, 472, 508, 546, 603, 612, 668, 695, 739, 778, 825, 872, 906, 931, 964, 1005, 1090, 1164, 1235, 1288, 1316, 1370, 1480, 1600, 1652, 1711, 1752, 1828, 1867, 1904, 1921, 2001, 2071, 2166, 2206, 2216, 2306, 2459, 2583, 2698, 2932, 3085, 3333, 3534, 3659, 3746, 3903, 3937, 3976, 4086, 4103, 4234, 4339, 4412, 4510, 5120, 5258, 5879, 5957, 6338, 6455, 6501, 6564, 6695, 6713, 6760, 6811, 6858, 6934, 6988, 7055, 7101, 7179, 7229, 7299, 7336, 7380, 7411, 7454, 7474, 7514, 7580, 7603, 7644, 7685, 7772, 7875, 7910, 8004, 8109, 8200};
        int[] quantileS = {0, 94, 172, 226, 277, 309, 360, 423, 498, 541, 611, 637, 668, 690, 718, 754, 793, 844, 890, 940, 982, 1022, 1117, 1187, 1267, 1301, 1384, 1472, 1559, 1663, 1716, 1763, 1830, 1889, 1947, 2017, 2094, 2176, 2314, 2437, 2545, 2642, 2810, 2912, 2961, 3095, 3225, 3294, 3404, 3497, 3631, 3723, 3775, 3852, 3960, 4027, 4162, 4262, 4353, 4413, 4516, 4678, 4778, 4871, 4990, 5126, 5225, 5314, 5417, 5493, 5633, 5751, 5845, 5950, 6104, 6230, 6319, 6411, 6490, 6549, 6641, 6759, 6853, 6938, 6996, 7080, 7173, 7239, 7348, 7427, 7483, 7540, 7603, 7680, 7781, 7847, 7894, 7952, 8026, 8090, 8200};

        ///zipf0.6
        int[] quantileR = {0, 7101,51454,182654,517370,840896,1361322,2354699,3520581,4508324,5639163,6786954,7863195,10020064,12090487,13679776,16287375,17612743,20643509,23012855,24905110,31208378,38145692,43867453,47839289,54318278,58393244,61340277,69087473,75527158,80040888,89430415,97554924,106477940,112207410,119543818,132727868,144747145,162099085,173430030,183227739,193843715,207279032,214555262,238173623,249284312,263512055,274255486,286455283,297927119,315483915,325151416,332366565,352312943,360932152,378163166,401709020,428343576,450295198,465331686,487671460,507329087,520431141,543616956,565664020,580388395,616792118,648300914,671507143,703171382,730741163,789862665,815812095,859628686,883923031,914651037,953509493,989349511,1013061118,1038719650,1093231848,1117979813,1146397363,1172515724,1214811677,1255107676,1281141532,1318515372,1378373494,1424256945,1485413554,1516903641,1581100896,1657654812,1711061929,1776793585,1795968520,1823961873,1921293519,1962428790,2000000000};
        int[] quantileS = {0, 7101,51454,182654,517370,840896,1361322,2354699,3520581,4508324,5639163,6786954,7863195,10020064,12090487,13679776,16287375,17612743,20643509,23012855,24905110,31208378,38145692,43867453,47839289,54318278,58393244,61340277,69087473,75527158,80040888,89430415,97554924,106477940,112207410,119543818,132727868,144747145,162099085,173430030,183227739,193843715,207279032,214555262,238173623,249284312,263512055,274255486,286455283,297927119,315483915,325151416,332366565,352312943,360932152,378163166,401709020,428343576,450295198,465331686,487671460,507329087,520431141,543616956,565664020,580388395,616792118,648300914,671507143,703171382,730741163,789862665,815812095,859628686,883923031,914651037,953509493,989349511,1013061118,1038719650,1093231848,1117979813,1146397363,1172515724,1214811677,1255107676,1281141532,1318515372,1378373494,1424256945,1485413554,1516903641,1581100896,1657654812,1711061929,1776793585,1795968520,1823961873,1921293519,1962428790,2000000000};

        long[][] rangeArrR = new long[_numInstanceR][2];
        long[][] rangeArrS = new long[_numInstanceS][2];

        for(int i = 0; i < _numInstanceR; i ++){
            rangeArrR[i][0] = quantileR[i];
            rangeArrR[i][1] = quantileR[i+1];
        }
        rangeArrR[0][0] = Long.MIN_VALUE + 1;
        rangeArrR[_numInstanceR-1][1] = Long.MAX_VALUE - 1;
        for(int i = 0; i < _numInstanceS; i ++){
            rangeArrS[i][0] = quantileS[i];
            rangeArrS[i][1] = quantileS[i+1];
        }
        rangeArrS[0][0] = Long.MIN_VALUE + 1;
        rangeArrS[_numInstanceR-1][1] = Long.MAX_VALUE - 1;
        //////////

 */

        initializeRouterTable(_routerTableR, _joinerRTaskIDs, rangeArrR);
        initializeRouterTable(_routerTableS, _joinerSTaskIDs, rangeArrS);
        for(int i = 0; i < _routerTableR.getItemNums(); i++){
            RouterItem routerItem = _routerTableR.getItems().get(i);
            output("After initialize:" + routerItem.start + "," + routerItem.end + "," + routerItem.task + ","+ routerItem.shard + "\n");
        }
        for(int i = 0; i < _routerTableS.getItemNums(); i++){
            RouterItem routerItem = _routerTableS.getItems().get(i);
            output("After initialize:" + routerItem.start + "," + routerItem.end + "," + routerItem.task + ","+ routerItem.shard + "\n");
        }

        _maxValue = 0L;
        _numRealEmitR = 0L;
        _numRealEmitS = 0L;
        _numReceiveR = 0L;
        _numReceiveS = 0L;
        _ES1R = 0L;
        _ES1S = 0L;
        ////TODO:统计历史数据，只用一次，后注释
        _countR = 0;
        _countS = 0;
/*        _keyR = new ArrayList<Integer>(424000);///stock的值域为3~423401，滴滴的值域：0-8200
        _keyS = new ArrayList<Integer>(424000);
        _quantileR = new ArrayList<Integer>(100+1);
        _quantileS = new ArrayList<Integer>(100+1);
        for(int i = 0; i < 100 + 1; i++){
            _quantileR.add(0);
            _quantileS.add(0);
        }
        for(int i = 0; i < 424000; i ++){
            _keyR.add(0);
            _keyS.add(0);
        }*/
    }

    @Override
    public void execute(Tuple tuple) {
        /* extract contents from the tuple */
        long currts = _stopwatch.elapsed(MICROSECONDS);
        if(!_initial){
            List<RouterItem<Long>> rRouterList= _routerTableR.getItems();
            for(int i = 0; i < rRouterList.size(); i++){
                RouterItem<Long> ithItem = rRouterList.get(i);
                Shard shardr = new Shard(ithItem.shard, ithItem.start, ithItem.end, ithItem.task); //type, rel, emiShardID, immTaskID, immShardID, minRange, cursor, maxRange
                _collector.emit(_streamShuffleR, new Values("initialShard", "null", 0L, 1, shardr.encodeShardInShuffle(), String.valueOf(ithItem.task), ithItem.shard));
//                _collector.emit(_streamReshuffleToMonitorR, new Values("initialShard", "R", Integer.MIN_VALUE, Long.MIN_VALUE, shardr._owner, shardr._shardID, shardr._minRange, shardr._maxRange, Long.MIN_VALUE));
                _collector.emit(_streamReshuffleToMonitorR, new Values("initialShard", "R", Integer.MIN_VALUE, Long.MIN_VALUE, ithItem.task, ithItem.shard, ithItem.start, ithItem.end, Long.MIN_VALUE, 0l));
            }
            List<RouterItem<Long>> sRouterList= _routerTableS.getItems();
            for(int i = 0; i < sRouterList.size(); i++){
                RouterItem<Long> ithItem1 = sRouterList.get(i);
                Shard shards = new Shard(ithItem1.shard, ithItem1.start, ithItem1.end, ithItem1.task);
//                output("shards.encodeShard()=" + shards.encodeShard() + ",ithItem1.task" + ithItem1.task);  "type", "relation", "emitTaskID", "emiShardID", "immTaskID", "immShardID", "minRange", "cursor", "maxRange"
                _collector.emit(_streamShuffleS, new Values("initialShard", "null", 0L, 1, shards.encodeShardInShuffle(), String.valueOf(ithItem1.task), ithItem1.shard));
//                _collector.emit(_streamReshuffleToMonitorS, new Values("initialShard", "R", Integer.MIN_VALUE, Long.MIN_VALUE, shards._owner, shards._shardID, shards._minRange, shards._maxRange, Long.MIN_VALUE));
                _collector.emit(_streamReshuffleToMonitorS, new Values("initialShard", "R", Integer.MIN_VALUE, Long.MIN_VALUE, ithItem1.task, ithItem1.shard, ithItem1.start, ithItem1.end, Long.MIN_VALUE, 0l));
            }
            _initial = true;
        }

        String type = tuple.getStringByField("type");
        String rel = tuple.getStringByField("relation");
        String taskIdShufferS = "", taskIdShufferR = "", taskIdBroadS2R = "", taskIdBroadR2S = "";

        ////TODO:统计历史数据，只用一次，后注释 这里都不对，回来检查~~~！！！！
/*        if(type.equals("tuple")){
            int step = 20;///8200/200;8200/400
            Long key = tuple.getLongByField("key");
            if(rel.equals("R")){
                _countR++;
                Long cur = key/step;
                int num = _quantileR.get(cur.intValue()) + 1;
                _quantileR.set(cur.intValue(), num);
            }else {
                _countS++;
                Long cur = key/step;
                int num = _quantileS.get(cur.intValue()) + 1;
                _quantileS.set(cur.intValue(), num);
            }
            final int nn = 50000;
            if(_countR == nn){
                output("\n" + "The number of R "  + ",maxValue = " + _maxValue+ "\n");
                for(int i = 0; i < 410 + 1; i++){
                    output(_quantileR.get(i) + " ");
                    _quantileR.set(i,0);
                }
                _countR = 0;
            }
            if(_countS == nn){
                output("\n" + "The number of S " + "\n");
                for(int i = 0; i < 410 + 1; i++){
                    output(_quantileS.get(i) + " ");
                    _quantileS.set(i,0);
                }
                _countS = 0;
            }
        }*/
/*        if(type.equals("tuple")){
//            output("the rel is:" + rel + "\n");
            if(rel.equals("R")){
                Long key = tuple.getLongByField("key");
//                output("R, key=" + key + ",_keyR.get(key.intValue())=" + _keyR.get(key.intValue()) + "\n");
                _keyR.set(key.intValue(),_keyR.get(key.intValue())+1);
                _countR++;
            }else {
                Long key = tuple.getLongByField("key");
//                output("S, key=" + key + ",_keyS.get(key.intValue())" + _keyS.get(key.intValue()) + "\n");
                _keyS.set(key.intValue(),_keyS.get(key.intValue())+1);
                _countS++;
            }
            final int nn = 500000;
            if(_countR == nn){
                int step = 0;
                output("_numInstanceR=" + _numInstanceR + ",_countR=" + _countR + "\n");
                _numInstanceR = 100;
                if(_numInstanceR > 0){
                    step = (int) _countR/_numInstanceR;
                }

                int sum = 0;
                int k = 1;
                for(int i = 0; i < _keyR.size(); i++){
                    sum += _keyR.get(i);
                    if(sum > k * step) {
                        _quantileR.add(i);
                        k++;
                    }
                }
                output("RouterR: " + _quantileR.toString() + "\n");
                _countR = 0;
            }
            if(_countS == nn){
                int step = 0;
                output("_numInstanceS=" + _numInstanceS + ",_countS=" + _countS+ "\n");
                _numInstanceS = 100;
                if(_numInstanceS > 0){
                    step = (int) _countS/_numInstanceS;
                }
                int sum = 0;
                int k = 1;
                for(int i = 0; i < _keyS.size(); i++){
                    sum += _keyS.get(i);
                    if(sum > k * step) {
                        _quantileS.add(i);
                        k++;
                    }
                }
                output("RouterS: " + _quantileS.toString() + "\n");
                _countS = 0;
            }
        }*/

//        output("-----------------key="+key);
        /* shuffle to store and broadcast to join */
        if(type.equals("tuple")){
            Long ts = tuple.getLongByField("timestamp");
            Long key = tuple.getLongByField("key");
//            key = (long) Math.floor((key-minrange)/(1<<_bits));
            String value = tuple.getStringByField("value");
            /////TODO:临时用的，后面会删掉
/*            if(rel.equals("R")){
                _countR++;
                if(key > _maxValue) {
                    _maxValue = key;
                }
                if(key < _minValue) {
                    _minValue = key;
                }
                if(_countR >= 50000){
                    output("_maxValue=" + _maxValue + "\n");
                    output("_minValue=" + _minValue + "\n");
                    _countR = 0;
                }
            }else if(rel.equals("S")){
                _countS++;
                if(key > _maxValueS) {
                    _maxValueS = key;
                }
                if(key < _minValueS) {
                    _minValueS = key;
                }
                if(_countS >= 50000){
                    output("_minValueS=" + _minValueS + "\n");
                    output("_maxValueS=" + _maxValueS + "\n");
                    _countS = 0;
                }
            }*/

            if (rel.equals("R")) {
                taskIdShufferR = _routerTableR.getTaskIDs(key);
                if(taskIdShufferR.equals(""))
                _routerTableR.printRouter("Router_R Error! @ key:" + key );
                Values values = new Values("tuple", rel, ts, key, value, taskIdShufferR, 0L);
//                if(taskIdShufferR.equals("")){output("taskIdShufferR,key=" + key + "\n");}
                _collector.emit(_streamShuffleR, values);
                taskIdBroadR2S = _routerTableS.getTaskIDs(key - _e1, key + _e2);
//                if(taskIdBroadR2S.equals("")){output("taskIdBroadR2S,key=" + key + "\n");}
                String taskIdBroad[] = taskIdBroadR2S.split(",");
                _numRealEmitR += taskIdBroad.length;
                Values values1 = new Values("tuple", rel, ts, key, value, taskIdBroadR2S, 0L);
                _collector.emit(_streamBroadcastR, values1);
                _ES1R +=  _stopwatch.elapsed(MICROSECONDS) - currts;
                _numReceiveR++;
            }
            else if(rel.equals("S")){ // rel.equals("S")
                taskIdShufferS = _routerTableS.getTaskIDs(key);
                Values values = new Values("tuple", rel, ts, key, value, taskIdShufferS, 0L);
                if(taskIdShufferS.equals("")){_routerTableS.printRouter("Router_S Error! @ key:" + key );}
                _collector.emit(_streamShuffleS, values);
                taskIdBroadS2R = _routerTableR.getTaskIDs(key - _e1, key + _e2);
                String taskIdBroad[] = taskIdBroadS2R.split(",");
                _numRealEmitS += taskIdBroad.length;
                Values values1 = new Values("tuple", rel, ts, key, value, taskIdBroadS2R, 0L);
                if(taskIdBroadS2R.equals("")){output("taskIdBroadS2R,key=" + key + "\n");}
                _collector.emit(_streamBroadcastS, values1);
                _ES1S +=  _stopwatch.elapsed(MICROSECONDS) - currts;
                _numReceiveS++;
            }
            //////每秒发送一次_numRealEmit，和ET; ImmutableList.of("type", "relation", "emitTaskID", "emiShardID", "immTaskID", "immShardID", "minRange", "cursor", "maxRange", "numMigrateTuple")
            if(_stopwatch.elapsed(MILLISECONDS) - _lastOutputTime > 1000){
                Values valuesR = new Values("test", "R", _numRealEmitR.intValue(), _ES1R, _numReceiveR, 0L, 0L, 0L, 0L, 0);
                _collector.emit(_streamReshuffleToMonitorR, valuesR);
                Values valuesS = new Values("test", "S", _numRealEmitS.intValue(), _ES1S, _numReceiveS, 0L, 0L, 0L, 0L, 0);
                _collector.emit(_streamReshuffleToMonitorS, valuesS);
                _numInstanceR = 0;
                _numInstanceS = 0;
                _ES1R = 0L;
                _ES1S = 0L;
                _numRealEmitR = 0L;
                _numRealEmitS = 0L;
                _numRealEmitR = 0L;
                _numRealEmitS = 0L;
                _numReceiveR = 0L;
                _numReceiveS = 0L;
            }
        }
        else if(type.equals("modify")){////
            Long tsMod = _stopwatch.elapsed(MILLISECONDS);
            int emitTaskId = tuple.getSourceTask();
            Long emiShardID = tuple.getLongByField("emiShardID");
            Long immTaskID = tuple.getLongByField("immTaskID");
            Long immShardID = tuple.getLongByField("immShardID");
            Long minRange = tuple.getLongByField("minRange"); ////
            Long cursor = tuple.getLongByField("cursor");
            Long maxRange = tuple.getLongByField("maxRange");////
            Long numMigrateTuple = tuple.getLongByField("numMigrateTuple");
            Long currTimeJoiner = tuple.getLongByField("currTime");
            Long currTime = System.currentTimeMillis();
            output( rel + ",emitTaskId=" + emitTaskId + ",minRange=" + minRange + ",cursor=" + cursor + ",maxRange=" + maxRange
                    + ",currTimeJoiner= " + currTimeJoiner + ",currTime= " + currTime + ",currTime-currTimeJoiner=" + (currTime-currTimeJoiner)+ "\n");
            /*DEBUG_LOG*/{
                output("  [ Modify ]  " + "\n");
                output("--------- _maxValue=" + _maxValue + "\n");
                output("from @rel.tsk: " +rel + emitTaskId + "\n");
                if(minRange == Long.MIN_VALUE){ ////向左迁移
                output( "@tsk-SD: " + immTaskID+ "-"+ immShardID + "<<<<<"
                        +"@tsk-SD: " + emitTaskId+ "-"+ emiShardID+"\n");
                }
                else if(minRange == Long.MAX_VALUE){///向右迁移
                    output( "@tsk-SD: " + emitTaskId+ "-"+ emiShardID + ">>>>>"
                            +"@tsk-SD: " + immTaskID+ "-"+ immShardID + "\n");
                }
                output(" cursor" +", " + "numMigrateTuple"  + "\n");
                output( cursor +", "+ numMigrateTuple + "\n");
/*                if(rel.equals("R")){
                    output(_routerTableR.printRouter("Before RouterR modify"));
                }else {
                    output(_routerTableS.printRouter("Before RouterS modify"));
                }*/
            }
            boolean debug_modifyRouterTable = false;
            StringBuilder debugSting = new StringBuilder();
            if(rel.equals("R")){
                if(minRange == Long.MIN_VALUE){ ////向左迁移
                    debug_modifyRouterTable = _routerTableR.modifyRouterTable(immShardID, emiShardID, cursor, debugSting);
                }else if(minRange == Long.MAX_VALUE){///向右迁移
                    debug_modifyRouterTable = _routerTableR.modifyRouterTable(emiShardID, immShardID, cursor, debugSting);
                }
                if(debug_modifyRouterTable)
                _collector.emit(_streamReshuffleToMonitorR, new Values(type, rel, emitTaskId, emiShardID, immTaskID.intValue(), immShardID, minRange, cursor, maxRange, numMigrateTuple));
            }else if(rel.equals("S")){
                if(minRange == Long.MIN_VALUE){ ////向左迁移
                    debug_modifyRouterTable = _routerTableS.modifyRouterTable(immShardID, emiShardID, cursor, debugSting);
                }else if(minRange == Long.MAX_VALUE){///向右迁移
                    debug_modifyRouterTable = _routerTableS.modifyRouterTable(emiShardID, immShardID, cursor, debugSting);
                }
                if(debug_modifyRouterTable)
                _collector.emit(_streamReshuffleToMonitorS, new Values(type, rel, emitTaskId, emiShardID, immTaskID.intValue(), immShardID, minRange, cursor, maxRange, numMigrateTuple));
            }

            /*DEBUG_LOG*/
/*            {
                if(rel.equals("R")){
                    output(_routerTableR.printRouter("After RouterR modify"));
                }else {
                    output(_routerTableS.printRouter("After RouterS modify"));
                }
                output( "modifyRouterTable()" +", "+ debug_modifyRouterTable + "\n");
                output(debugSting.toString() + "\n");
                output("  [-]  " + "\n");
            }*/
            output("the time of modify:" + (_stopwatch.elapsed(MILLISECONDS)-tsMod) + "\n");
        }
        else if(type.equals("shard")){////type", "relation", "emiShardID", "immTaskID", "immShardID", "minRange", "cursor", "maxRange", "numMigrateTuple"
            Long tsShard = _stopwatch.elapsed(MILLISECONDS);
            int emitTaskId = tuple.getSourceTask();
            Long emiShardID = tuple.getLongByField("emiShardID");
            int immTaskID = tuple.getLongByField("immTaskID").intValue();
            Long immShardID = tuple.getLongByField("immShardID");
            Long minRange = tuple.getLongByField("minRange"); ////
            Long cursor = tuple.getLongByField("cursor");
            Long maxRange = tuple.getLongByField("maxRange");////
            Long numMigrateTuple = tuple.getLongByField("numMigrateTuple");
            Long currTimeJoiner = tuple.getLongByField("currTime");
            Long currTime = System.currentTimeMillis();
            output( rel + ",emitTaskId=" + emitTaskId + ",minRange=" + minRange + ",cursor=" + cursor + ",maxRange=" + maxRange
                    + ",currTimeJoiner= " + currTimeJoiner + ",currTime= " + currTime + ",currTime-currTimeJoiner=" + (currTime-currTimeJoiner)+ "\n");
            /*DEBUG_LOG*/{
                output("  [ New Shard ]  " + "\n");
                output("--------- _maxValue=" + _maxValue + "\n");
                output("from @rel.tsk: " +rel + emitTaskId + "\n");
                output( "@tsk-SD: " + immTaskID+ "-"+ immShardID + "<<~~~~~<<<"
                        +"@tsk-SD: " + emitTaskId+ "-"+ emiShardID+"\n");
                output(" minRange" +", " + "cursor" +", " + "maxRange"+", " + "numMigrateTuple" + "\n");
                output( minRange +", "+ cursor+", "+ maxRange+", "+ numMigrateTuple + "\n");
/*                if(rel.equals("R")){
                    output(_routerTableR.printRouter("Before RouterR new shard"));
                }else {
                    output(_routerTableS.printRouter("Before RouterS new shard"));
                }*/
            }
            if(rel.equals("R")){ ///long emigrateShardId, long newShardId, int newShardTasksId, K newShardStart, K newShardEnd
                if(!_routerTableR.modifyNShardRouterTable(emiShardID, immShardID, immTaskID, minRange, cursor))
                    output("_routerTableR.modifyNShardRouterTable() false fail " + emiShardID + "," + immShardID + "," + immTaskID + "," + minRange + "," + cursor + "\n");
                else {
                    output("_routerTableR.modifyNShardRouterTable() true success " + emiShardID + "," + immShardID + "," + immTaskID + "," + minRange + "," + cursor + "\n");
                    _collector.emit(_streamReshuffleToMonitorR, new Values(type, rel, emitTaskId, emiShardID, immTaskID, immShardID, minRange, cursor, maxRange, numMigrateTuple));
                }
            }else if(rel.equals("S")){
                if(!_routerTableS.modifyNShardRouterTable(emiShardID, immShardID, immTaskID, minRange, cursor))
                    output("_routerTableS.modifyNShardRouterTable() false fail " + emiShardID + "," + immShardID + "," + immTaskID + "," + minRange + "," + cursor + "\n");
                else {
                    output("_routerTableR.modifyNShardRouterTable() true success " + emiShardID + "," + immShardID + "," + immTaskID + "," + minRange + "," + cursor + "\n");
                    _collector.emit(_streamReshuffleToMonitorS, new Values(type, rel, emitTaskId, emiShardID, immTaskID, immShardID, minRange, cursor, maxRange, numMigrateTuple));
                }
            }
            /*DEBUG_LOG*/{
/*                if(rel.equals("R")){
                    output(_routerTableR.printRouter("After RouterR new shard"));
                }else {
                    output(_routerTableS.printRouter("After RouterS new shard"));
                }*/
                output("the time of Shard:" + (_stopwatch.elapsed(MILLISECONDS)-tsShard) + "\n");
                output("  [-]  " + "\n");
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(_streamShuffleR, new Fields(SCHEMA));
        declarer.declareStream(_streamShuffleS, new Fields(SCHEMA));
        declarer.declareStream(_streamBroadcastR, new Fields(SCHEMA));
        declarer.declareStream(_streamBroadcastS, new Fields(SCHEMA));
//        declarer.declareStream(_streamMonitorR, new Fields(TO_MONITOR_SCHEMA));
//        declarer.declareStream(_streamMonitorS, new Fields(TO_MONITOR_SCHEMA));
        declarer.declareStream(_streamReshuffleToMonitorR, new Fields(TO_MONITOR_SCHEMA));
        declarer.declareStream(_streamReshuffleToMonitorS, new Fields(TO_MONITOR_SCHEMA));
    }

    private void initializeRouterTable(RouterTable routerTable, List<Integer> joinerTaskIDs,  long[][] iniArray){
        int step = 1;
        int location = 0, startLocation = 0;
        for (int i = 0; i < iniArray.length; i++){
            RouterItem<Long> testItem = new RouterItem<Long>();
            testItem.start = iniArray[i][0];
            testItem.end = iniArray[i][1];
            testItem.task = joinerTaskIDs.get(location); ////TODO:轮训？
            testItem.shard = (testItem.task<<20) | (testItem.start & 0xFFFFFFFFFFFFL);//testItem.task<<52 | (ts/1000)<<20 | ((long)((1L<<20)*Math.random()) & 0xFFFFF);
            routerTable.addItem(testItem);
            location += step;
            if(location >= joinerTaskIDs.size()){
                startLocation += 1;
                location = startLocation;
            }
            if((i+1) % joinerTaskIDs.size() == 0){
                step *= 2;
                location = 0;
                startLocation = 0;
            }
        }
    }


    private void output(String msg) {
        if (_output != null)
            _output.writeImmediately(msg);
    }
    @Override
    public void cleanup() {
        if (_output != null) {
            _output.endOfFile();
        }
    }

/*
    +routerTable//转发路由表
 +getTarget()//路由
 +initializeRouterTable()//初始化路由表+SD部署
 -modifyRouterTable()//接受路由表更新
 -sendModifyBarrier()//路由表修改同步
*/

}