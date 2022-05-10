package soj.biclique.bolt;

import java.text.DecimalFormat;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;

import com.google.common.collect.Lists;
import javafx.scene.layout.Priority;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;

import static java.util.concurrent.TimeUnit.*;
import static org.slf4j.LoggerFactory.getLogger;

import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import static com.google.common.collect.Lists.newLinkedList;

import soj.biclique.Shard.Shard;
import soj.util.FileWriter;
import soj.util.Stopwatch;
import static soj.util.CastUtils.getBoolean;
import static soj.util.CastUtils.getInt;
import static soj.util.CastUtils.getList;
import static soj.util.CastUtils.getLong;
import static soj.util.CastUtils.getString;
import static soj.biclique.Shard.Shard.*;
import static soj.biclique.KafkaTopology.JOINER_TO_POST_STREAM_ID;
import static soj.biclique.KafkaTopology.JOINER_TO_JOINER_STREAM_ID;
import static soj.biclique.KafkaTopology.JOINER_TO_JOINER_MODIFY_STREAM_ID;
import static soj.biclique.KafkaTopology.JOINER_TO_RESHUFFLER_STREAM_ID;
import static soj.biclique.KafkaTopology.JOINER_TO_MONITOR_STREAM_ID;
import static soj.biclique.KafkaTopology.MONITOR_R_STREAM_ID;
import static soj.biclique.KafkaTopology.MONITOR_S_STREAM_ID;
public class JoinBolt extends BaseBasicBolt
{
//    private static final List<String> TO_POST_SCHEMA = ImmutableList.of("currentMoment",
//            "tuples", "joinTimes", "processingDuration", "latency", "migrationTime", "migrationTuples", "resultNum", "load");
    private static final List<String> TO_POST_SCHEMA = ImmutableList.of("currentMoment",
        "tuplesStored", "joinTimes", "processingDuration", "latency", "migrationTime", "tuplesJoined", "resultNum", "load");

    private static final List<String> TO_JOINER_SCHEMA = ImmutableList.of("type", "relation", "timestamp", "key", "value", "boltID", "shardID");
//    private static final List<String> TO_JOINER_SCHEMA = ImmutableList.of("type", "timestamp", "num", "value", "info");
    private static final List<String> TO_JOINER_MODIFY_SCHEMA = ImmutableList.of("type", "immShardID", "leftRightOrJump", "minRange", "cursor", "immTaskID"); //""modify", immigrateShardID, leftRightOrJump, "null", tmpCursor
    private static final List<String> TO_RESHUFFLER_SCHEMA = ImmutableList.of( "type", "relation", "emiShardID", "immTaskID", "immShardID", "minRange", "cursor", "maxRange", "numMigrateTuple", "currTime");///"shard", _taskRel, emigrateShardID, immTaskID, immigrateShardID, minRange, tmpCursor, maxRange
    private static final List<String> TO_MONITOR_SCHEMA = ImmutableList.of("type", "taskrel", "timestamp", "statistics");
//    private static final List<String> TO_MONITOR_SCHEMA = ImmutableList.of("type", "relation", "key", "target");
    private static final long PROFILE_REPORT_PERIOD_IN_SEC = 1;
    private static final int BYTES_PER_TUPLE_R = 64;
    private static final int BYTES_PER_TUPLE_S = 56;
    private static final int BYTES_PER_MB = 1024 * 1024;

    private static final Logger LOG = getLogger(JoinBolt.class);
    private String _taskRel;

    private double _interval;


    private int _tid;

    private FileWriter _output;

    private int _thisJoinFieldIdx;
    private int _oppJoinFieldIdx;
    private String _operator;

    private boolean _window;

//    private Map<Long, Long>  _relKeyCounter;
//    private Map<Long, Long>  _oppRelKeyCounter;
    private Map<Integer, ArrayList<Values>> _buffer;

    private long _profileReportInSeconds;
    private long _triggerReportInSeconds;
    private Stopwatch _stopwatch;
    private DecimalFormat _df;

    private long _joinedTime;
    private long _lastJoinedTime;
    private long _lastOutputTime;
    private long _lastProcessTuplesNum;
    private double _latency;
    private boolean _begin;
    private int _thisTupleSize;
    private int _oppTupleSize;
    private long _resultNum;
    private long _lastResultNum;
    private long _migrationTimes;
    private long _migrationTuples;
    private long _lastReceiveTuples;
    private long _currReceiveTuples;
    private TopologyContext _context;
    private long _load, _preload;
    private long _nextTime;
    private int _subindexSize;
    private boolean _initial;
    private long _arrTuplesJoined, _tuplesJoined;
    private long _tuplesStoredPost;
    private int _storeSampleStep;//分别为连接元组和存储元组采样排序的抽样比。其中_joinSampleStep为存储的时候就是抽样存储的，所以不用在此二次抽样。_joinSampleStep,
    private double _penaltyAlpha, _rewardAlpha;
    private int _numShard, _numInitPerTask;
    private long _lastTime;
    private long _thisWinSize;
    private long _e1, _e2;
    private boolean _continuousM, _jumpM;
    private double  _oldMiu2, _miu2, _miu2A, _lamda2, _lamda2A, _consProcess;
    private double _Miu[];
    private int _countSec;

    List<Shard> _shardContainer = new ArrayList<Shard>(1);


    public JoinBolt(String relation, double interval, boolean win, long winsize, long e1, long e2, int numShard,
                    boolean continuousM, boolean jumpM, double penaltyAlpha, double rewardAlpha, int numInitPerTask) {
        super();
        _taskRel = relation;
        _interval = interval;
        _window = win;
        _thisWinSize = winsize;
        _e1 = e1;
        _e2 = e2;
        _numShard = numShard;
        _continuousM = continuousM;
        _jumpM = jumpM;
        _penaltyAlpha = penaltyAlpha;
        _rewardAlpha = rewardAlpha;
        _numInitPerTask = numInitPerTask;

        checkState(_taskRel.equals("R") || _taskRel.equals("S"), "Unknown relation: "
                + _taskRel);
    }

    @Override
    public void prepare(Map conf, TopologyContext context) {
        _tid = context.getThisTaskId();
        _context = context;
        String prefix = "joiner_" + _taskRel.toLowerCase() + _tid;
//        _output = new FileWriter("/home/shuiyinyu/join/tmpresult-fastjoin", prefix, "csv");///.setFlushSize(1);//..setPrintStream(System.out);

        _subindexSize = getInt(conf.get("subindexSize"));

        LOG.info("relation:" + _taskRel + ", join_field_idx(this):"
                + _thisJoinFieldIdx + ", join_field_idx(opp):"
                + _oppJoinFieldIdx + ", operator:" + _operator + ", window:"
                + _window + ", win_size:" + _thisWinSize + ", subindex_size:"
                + _subindexSize);

        /* profiling */

        _joinedTime = 0;
        _lastJoinedTime = 0;
        _lastOutputTime = 0;
        _lastProcessTuplesNum = 0;
        _latency = 0;
        _begin = true;
        _resultNum = 0;
        _lastResultNum = 0;

        _df = new DecimalFormat("0.00");
        _profileReportInSeconds = PROFILE_REPORT_PERIOD_IN_SEC;
        _triggerReportInSeconds = _profileReportInSeconds;
        _stopwatch = Stopwatch.createStarted();
//        _relKeyCounter = new HashMap<Long, Long>();
//        _oppRelKeyCounter = new HashMap<Long, Long>();
        _migrationTimes = 0L;
        _migrationTuples = 0L;
        _lastReceiveTuples = 0L;
        _currReceiveTuples = 0L;
        _load = 0L;
        _preload = 0L;
        _nextTime = _stopwatch.elapsed(SECONDS);
        _initial = false;
//        _arrTuplesJoined = 0l;
//        _joinSampleStep = 30;
        _storeSampleStep = 30;
        _tuplesJoined= 0;
//        _alpha1 = 3;
//        _alpha2 = 0.6;
        _lastTime = 0l;
        _tuplesStoredPost = 0l;
        _miu2 = _miu2A = _lamda2 = _lamda2A = 0.0;
//        _attenuationCoe = 0.86;
        _Miu = new double[(int)_thisWinSize/(2*1000)];
        for(int i = 0; i < (int)_thisWinSize/(2*1000); i++){
            _Miu[i] = -1.0;
        }
        _countSec = 0;
        _consProcess = 30000000.0;
    }


    public void execute(Tuple tuple, BasicOutputCollector collector) {
//        return; ////为了测试数据分布注释
        String type = tuple.getStringByField("type"); ///"type", "relation", "timestamp", "key", "value", "boltID", "shardID"

        if(type.equals("tuple")){
            String rel = tuple.getStringByField("relation");
            Long key = tuple.getLongByField("key");
            Long ts = tuple.getLongByField("timestamp");
            boolean belongTask = true;
//            List<Long> shardIDs = findShardID(_shardContainer,rel,key);
            for(int i = 0; i < _shardContainer.size(); i++){
                Shard shard = _shardContainer.get(i);
                if(rel.equals(_taskRel)){
                    if(belongShard(shard._minRange, shard._maxRange, rel, key)) {
                        shard.store(tuple);
                        if(checkTimeStamp(ts))
                        _tuplesStoredPost ++;
                    }
                }
                else {
//                    _arrTuplesJoined ++;
                    if(belongShardForJoin(shard._minRange, shard._maxRange, rel, key)){
                        shard.join(tuple,collector);
                        _tuplesJoined ++;
                        _latency += (System.currentTimeMillis() - ts);///这个延迟表示连接操作的延迟
                    }
                }
            }
        }
        else if(type.equals("shard")){///joiner to joiner new Shard  :"shard", "null", leftRightOrJump, minRange, tmpCursor  SCHEMA: "type", "immShardID", "leftRightOrJump", "minRange", "cursor", "immTaskID"
//            output("[Receive shard]" + "\n");
            Long shardID = tuple.getLongByField("immShardID");
            Long minRange = tuple.getLongByField("minRange");
            Long cursor = tuple.getLongByField("cursor");
            Long owner = tuple.getLongByField("immTaskID");
            Shard sha = new Shard(shardID, minRange, cursor, owner, _subindexSize, _window, _thisWinSize);
            _shardContainer.add(sha);
            for(int i = 0; i < _Miu.length-1; i++){ ///因为有元组迁入，为了减少之前的ES的影响，把Miu清空，重新计算。
                _Miu[i] = -1.0;
            }
            /*DEBUG_LOG*/{
/*                output("  [ Receive & add Shard ]  " + "\n");
                output("from @tsk-SD: " +tuple.getSourceTask() + "-" + shardID + "\n");
                output(" minRange " +", " + "maxRange" +", "+ "owner" + "\n");
                output( minRange +", " + cursor +", "+ owner + "\n");
                output("  [-]  " + "\n");*/
            }
        }
        else if(type.equals("migration")){ ////开始迁移
            ////计算收益，操作有，给存储元组和连接元组排序，收益计算。 "type", "minLoadId", "minLoad", "nbIdsLoads"
            Long minLoadId = tuple.getLongByField("minLoadId");
            String minLoad = tuple.getStringByField("minLoad");
            String nbLowShard = tuple.getStringByField("nbLowShard");////???？？？不一定对
            Double B1_AddShard = tuple.getDoubleByField("B1AddSh");
            Double B1_ReduceShard = tuple.getDoubleByField("B1ReSh");
            Double n0 = tuple.getDoubleByField("n0");
            long currTime = System.currentTimeMillis();

            /*DEBUG_LOG*/{
                output("  [ Start migration ]  CurrTime:" + currTime + "\n");
            }
            migrate(minLoadId, minLoad, nbLowShard, B1_AddShard, B1_ReduceShard, n0, collector);
            /*DEBUG_LOG*/{
                output("  [-]  " + "\n");
            }
        }
        else if(type.equals("modify")){  ///"type", "immShardID", "leftRightOrJump", "minRange", "maxRange"
            Long shardID = tuple.getLongByField("immShardID");
            String leftOrRight = tuple.getStringByField("leftRightOrJump");
            Long cursor = tuple.getLongByField("cursor");
            for(int i = 0; i < _Miu.length-1; i++){ ////把Miu清空
                _Miu[i] = -1.0;
            }
/*            *//*DEBUG_LOG*//*{
                output("  [ Receive Modify ]  " + "\n");
                output("from @tsk: " +tuple.getSourceTask() + "\n");
                output(" shardID " +", " +" leftOrRight " +", " + "cursor"  + "\n");
                output( shardID +", " + leftOrRight +", " + cursor + "\n");
                output("  [-]  " + "\n");
            }*/
            boolean debug_modifyShard = false;
            Long oldMin = 0l,oldMax = 0l,newMin = 0l,newMax = 0l;
            for(int i = 0; i < _shardContainer.size(); i++){ ////shard应该用map来管理。。。。
                if(_shardContainer.get(i)._shardID == shardID){
                    if(leftOrRight.equals("left")){
                        oldMin = _shardContainer.get(i)._minRange ;
                        oldMax = _shardContainer.get(i)._maxRange ;
                        debug_modifyShard = _shardContainer.get(i).modifyShard(leftOrRight, 0l, cursor);
                        newMin = _shardContainer.get(i)._minRange ;
                        newMax = _shardContainer.get(i)._maxRange ;
                        /*DEBUG_LOG*/{
//                            output(" [" + oldMin + ", " + oldMax + ")" +"-> " + " [" + newMin + ", " + newMax + ")"  + "\n");
//                            output( "modifyShard()  " + debug_modifyShard + "\n");
                        }
                    }else if(leftOrRight.equals("right")){ ///"modify", immigrateShardID, leftRightOrJump, "null", tmpCursor, "null"
                        oldMin = _shardContainer.get(i)._minRange ;
                        oldMax = _shardContainer.get(i)._maxRange ;
                        debug_modifyShard = _shardContainer.get(i).modifyShard(leftOrRight, cursor, 0l);
                        newMin = _shardContainer.get(i)._minRange ;
                        newMax = _shardContainer.get(i)._maxRange ;
                        /*DEBUG_LOG*/{
//                            output(" [" + oldMin + ", " + oldMax + ")" +"-> " + " [" + newMin + ", " + newMax + ")"  + "\n");
//                            output( "modifyShard()  " + debug_modifyShard + "\n");
                        }
                    }
                }
            }
            /*DEBUG_LOG*/{
                output("  [-]  " + "\n");
            }
        }
        else if(type.equals("imJoinT")){//"type", "nouse", "timestamp", "key", "nouse", "nouse", "shardID"
            Long numImmJoinTuple = tuple.getLongByField("key");
            Long shardID = tuple.getLongByField("shardID");
            Long ts = tuple.getLongByField("timestamp");
            for(int i = 0; i < _shardContainer.size(); i++){
                if(_shardContainer.get(i)._shardID == shardID){
                    _shardContainer.get(i).addImmJoinTupleNum(ts, numImmJoinTuple);
                    break;
                }
            }
        }
        else if(type.equals("initialShard")) {////第一次新建shard，再次因为jump迁移，new Shard "type", "relation", "timestamp", "key", "value", "boltID", "shardID"
            boolean findShard = false;
            String value = tuple.getStringByField("value");
            String []sh = value.split(",");
            for(int i = 0; i < _shardContainer.size(); i++){
                if(_shardContainer.get(i)._shardID == Long.parseLong(sh[0])) { ///if(_shardContainer.get(i)._minRange == Long.parseLong(sh[1]) && _shardContainer.get(i)._maxRange == Long.parseLong(sh[2]))
                    findShard = true;
                    break;
                }
            }
//                Long shardID = tuple.getLongByField("shardID");
            if(!findShard){
                Shard sha = new Shard(value, _window, _thisWinSize, _subindexSize);
//                output("sha._shardID= " + sha._shardID + ",sha._minRange=" + sha._minRange + ",sha._maxRange=" + sha._maxRange + "\n");
                _shardContainer.add(sha);
                /*DEBUG_LOG*/{
//                    output("  [ InitialShard ]  @SD:  " +sha._shardID+ " [" + sha._minRange + ", " + sha._maxRange + ")"+  "\n");
                }
            }
        }

        if (isTimeToOutputProfile()) {
//            long ES2 = _stopwatch.elapsed(MICROSECONDS) - _lastSecMicro;
            /*if(_tuplesJoined > 0){
                _ES2 = _totalES2/_tuplesJoined;
                _countSec++;
                double miu2 = ((double) 1/_ES2) * 1000000;
                _Miu[_countSec%_Miu.length] = miu2;
            }
            double miu = 0.0;
            int countM = 0;
            for(double x : _Miu){
                if(x > 0){
                    miu+=x;
                    countM ++;
                }
            }
            _miu2 = miu/countM;
//            _lamda2 = _tuplesJoined  + _lamda2 * 0.86;///目前没用这个lamda
            _totalES2 = 0L;*/
            long interval_currts = _stopwatch.elapsed(MILLISECONDS);
            StringBuilder sb = new StringBuilder();


            double totalStoreT = 0.0, totalJoinT = 0.0, totalLoad = 1.0;
            for(int i = 0; i < _shardContainer.size(); i++){
                Shard shard = _shardContainer.get(i);
                if(i > 0){
                    sb.append(";");
                }
                sb.append(shard.encodeShardInJoiner());
                totalLoad += shard._numTuplesStored * shard._numTuplesJoined;
                totalStoreT += shard._numTuplesStored;
            }

            sb.append(";");

            _miu2 = 0.0;
            if(totalLoad ==0)totalLoad = 1;
            double consDivTatalLoad = _consProcess/totalLoad;
            for(int i = 0; i < _shardContainer.size(); i++){
                Shard shard = _shardContainer.get(i);
                if(shard._numTuplesStored == 0)shard._numTuplesStored = 1;
                shard._load = shard._numTuplesStored * shard._numTuplesJoined;
                _miu2 += (shard._load * consDivTatalLoad)/shard._numTuplesStored;
            }
            Double tmp = _miu2;
            sb.append(tmp.toString());

            String shardStatisticsMiu = sb.toString();
//            output("shardStatisticsMiu = " + shardStatisticsMiu + "\n");
            /*DEBUG_LOG*/{
                output("_miu2=" + _miu2 + ", totalStoreT=" + totalStoreT + ",totalLoad=" + totalLoad + ",_tuplesJoined=" + _tuplesJoined+ "\n");
//                output("  [ miu Report ]  @sec " +_stopwatch.elapsed(SECONDS) + ", _ES2=" + _ES2 + ", _miu2=" + _miu2 + "\n");
            }

            if (_taskRel.equals("R"))
                collector.emit(MONITOR_R_STREAM_ID, new Values("recordSignal-bolt", "R", interval_currts, shardStatisticsMiu));
            else if (_taskRel.equals("S"))
                collector.emit(MONITOR_S_STREAM_ID, new Values("recordSignal-bolt", "S", interval_currts, shardStatisticsMiu));
            long moment = _stopwatch.elapsed(SECONDS);
            long joinTimes = _joinedTime - _lastJoinedTime;
            long processingDuration1 = 1;
//            collector.emit(JOINER_TO_POST_STREAM_ID, new Values(moment, getShardStoredT(_shardContainer), joinTimes, processingDuration1, _latency, _migrationTimes, currTuplesJoined, _migrationTuples, _load));
            collector.emit(JOINER_TO_POST_STREAM_ID, new Values(moment, _tuplesStoredPost, joinTimes, processingDuration1, _latency, _migrationTimes, _tuplesJoined, _migrationTuples, _load));

/*            *//*DEBUG_LOG*//*{
                output(" interval_currts " +", " +" _tuplesJoined "  + "\n");
                output( interval_currts +", " + _tuplesJoined + "\n");
                output("  [-]  " + "\n");
            }*/
            _latency = 0;
            _tuplesJoined = 0;
//            _arrTuplesJoined = 0;
            _tuplesStoredPost = 0;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(JOINER_TO_POST_STREAM_ID, new Fields(TO_POST_SCHEMA));
        declarer.declareStream(JOINER_TO_RESHUFFLER_STREAM_ID, new Fields(TO_RESHUFFLER_SCHEMA));
//        declarer.declareStream(JOINER_TO_MONITOR_STREAM_ID, new Fields(TO_MONITOR_SCHEMA));
        declarer.declareStream(MONITOR_R_STREAM_ID, new Fields(TO_MONITOR_SCHEMA));
        declarer.declareStream(MONITOR_S_STREAM_ID, new Fields(TO_MONITOR_SCHEMA));
        declarer.declareStream(JOINER_TO_JOINER_STREAM_ID,true,new Fields(TO_JOINER_SCHEMA));
        declarer.declareStream(JOINER_TO_JOINER_MODIFY_STREAM_ID,true,new Fields(TO_JOINER_MODIFY_SCHEMA));
    }

    @Override
    public void cleanup() {
        _stopwatch.stop();

        StringBuilder sb = new StringBuilder();

        sb.append("relation:" + _taskRel);
//        sb.append(", num_of_indexes:" + (_indexQueue.size() + 1));
        sb.append(", window:" + _window + " , win_size:" + _thisWinSize + ", subindex_size:" + _subindexSize);

        output(sb.toString());

        if (_output != null)
            _output.endOfFile();
    }

    private <K extends Comparable<K>> boolean hasOverlap(K start, K end, boolean endClosed, K shardStart, K shardEnd){
        if (start.compareTo(end) > 0) return false;//选中范围无效
        if(endClosed == false && start.compareTo(end) == 0)
            return false;//选中范围无效
        if(endClosed == true)   // [,] (,]
            return !(end.compareTo(shardStart) < 0 || start.compareTo(shardEnd) >= 0);
        else    // [,) (,)
            return !(end.compareTo(shardStart) <= 0 || start.compareTo(shardEnd) >= 0);
    }


    private List<Long> findShardID(List<Pair> shardContainer, String rel, Long key){
        List<Long> shardIDs = new ArrayList<Long>(1);
        for(int i = 0; i < shardContainer.size(); i++){
            Shard shard = (Shard)shardContainer.get(i).getRight();
            if(rel.equals(_taskRel)){
                if(hasOverlap(key, key, true, shard._minRange, shard._maxRange)) shardIDs.add(shard._shardID);
            }else {
                if(hasOverlap(key-_e1, key+_e2, true, shard._minRange, shard._maxRange)) shardIDs.add(shard._shardID);
            }
        }
        return shardIDs;
    }


    private boolean belongShard(Long minRange, Long maxRange, String rel, Long key){
        if(hasOverlap(key, key, true, minRange, maxRange))return true;
        return false;
    }

    private boolean belongShardForJoin(Long minRange, Long maxRange, String rel, Long key){
        if(hasOverlap(key-_e1, key+_e2, true, minRange, maxRange))return true;
        return false;
    }

    @SuppressWarnings("unchecked")
    private Collection<Values> getMatchings(Object index, Object value) {
        return ((Multimap<Object, Values>) index).get(value);
    }


    @SuppressWarnings("unchecked")
    private Collection< Map.Entry<Object, Values> > getEntries(Object index) {
        return ((Multimap<Object, Values>) index).entries();
    }


    @SuppressWarnings("unchecked")
    private int getIndexSize(Object index) {
        return ((List<Values>) index).size();
    }

    @SuppressWarnings("unchecked")

    private boolean isTimeToOutputProfile() {
        long currTime = _stopwatch.elapsed(SECONDS);

        if (currTime >= _triggerReportInSeconds) {
            _triggerReportInSeconds = currTime + _profileReportInSeconds;
            return true;
        }
        else {
            return false;
        }
    }

//////getLoads
    private long getShardStoredT(List<Shard> shardContainer){
        long storedTuple = 0l;
        for(int i = 0; i < shardContainer.size(); i++){
            Shard sha = shardContainer.get(i);
            storedTuple = storedTuple + sha.getNumStored();
        }
        return storedTuple;
    }

    private long getTaskLoad(){
        long load = 0l;
        for(int i = 0; i < _shardContainer.size(); i++){
            Shard sha = _shardContainer.get(i);
            load += sha._numTuplesStored * sha._numTuplesJoined;
        }
        return load;
    }

    private void output(String msg) {
        if (_output != null){
            //_output.write(msg);
            _output.writeImmediately(msg);
        }
    }


    private void sendLoadReport(){

    }
//ET2,immET2, miu2A, lamda2A, immMiuA, immLamda)
    private double computeLoadBanlanceProfit(double ET2, double lamda2,
                                           double immET2, double immLamda, double miu2A, double lamda2A, double immMiuA, double immLamdaA){
        //benefit = (ET2 + immET2)-(ET2A + immET2A)
        double ETA = 1/(miu2A - lamda2A);
        double immETA = 1/(immMiuA - immLamdaA);
        double loadBanlanceProfit = ((ET2*lamda2 + immET2*immLamda)- (ETA*lamda2A + immETA * immLamdaA))/(lamda2+immLamda);
//        double loadBanlanceProfit = ((ET2 + immET2)- (ETA + immETA));
        return loadBanlanceProfit;
    };


    private void migrate(Long minId , String minLoad, String nbLowShard, Double B1_Addshard, Double B1_ReduceSha, Double n0, BasicOutputCollector collector){ //0104重写
        //采样结构
        Long ts = System.currentTimeMillis();
        List<Long> sortStoreTuple =  new ArrayList<>();
        Long taskLoadSelf = getTaskLoad();

        //解析迁移建议
        String []minTaskLoad = minLoad.split(",");
        String []nbLowSha = nbLowShard.split(",");
        output("------ minTaskLoad=" + minLoad + ",nbLowShard=" + nbLowShard + "\n");
        double joinWinToSec = (_thisWinSize/2.0 + 1)/1000;
        double storeWinToSec = (_thisWinSize/1000.0);

        Long immJumpTaskID       = Long.parseLong(minTaskLoad[0]);
        Long immJumpTaskShardNum = Long.parseLong(minTaskLoad[1]);
        Long immJumpTaskLoad     = Long.parseLong(minTaskLoad[2]);
        double immJumpMiu        = Double.parseDouble(minTaskLoad[3]);
        Long immJumpTotalJoinT   = Long.parseLong(minTaskLoad[4]);
        Long immJumpTotalStoredT = Long.parseLong(minTaskLoad[5]);
        if(immJumpTotalStoredT <= 0){
            immJumpTotalStoredT  = 1L;
        }

        Long immigrateNBShardID = Long.parseLong(nbLowSha[0]);
        Long immMinRangeNB      = Long.parseLong(nbLowSha[1]);
        Long immMaxRangeNB      = Long.parseLong(nbLowSha[2]);
        Long immNBShardStore = Long.parseLong(nbLowSha[4]);
        Long immNBShardJoin = Long.parseLong(nbLowSha[5]);
        Long emTaskIdNB        = Long.parseLong(nbLowSha[6]);
        Long emigrateNBShardID = Long.parseLong(nbLowSha[7]);
        String leftRightNB     = String.valueOf(nbLowSha[8]);
        Long immNBTaskID       = Long.parseLong(nbLowSha[9]);
        Long immNBTaskLoad     = Long.parseLong(nbLowSha[10]);///numTmTupleStoreNB * nuupleJoinNB;///
        double immNBMiu        = Double.parseDouble(nbLowSha[11]);
        Long immNBTotalJoinT   = Long.parseLong(nbLowSha[12]);
        Long immNBTotalStoreT  = Long.parseLong(nbLowSha[13]);
        if(immNBTotalStoreT <= 0){
            immNBTotalStoreT   = 1L;
        }

        //jump migration
        long JUMP_profit = -1L;
        double JUMP_profit_Q = -1.0;
        long JUMP_emigrateShardID = 0L;
        long JUMP_immTaskID = 0L;
        long JUMP_immigrateShardID = 0L;
        long JUMP_minRange = 0L;
        long JUMP_cursor = 0L;
        long JUMP_maxRange = 0L;

        //migrate to neighbor
        long NB_profit = -1L;
        double NB_profit_Q = -1.0;
        long NB_emigrateShardID = 0;
        long NB_immTaskID = 0;
        long NB_immigrateShardID = 0;
        long NB_minRange = 0L;
        long NB_cursor = 0L;
        long NB_maxRange = 0L;
        long totalNumJoinedT = 0L;
        long totalNumStoreT = 0L;
        double pow = -1;
        long totalTaskLoad = 0L;

        /*DEBUG_LOG*/{
/*            output("-----------migrate(): Recv Suggestion-------------" + "\n");
            output(" Tsk" +", " + "SD" + ", " + " <<<~~~<<<" + ", " + "Tsk" + ", " + "SD" + "\n");
            output( immJumpTaskID +", " + "?" +", "+ "<<<~~~<<<" +", " + _tid +", "+ "?"+ "\n");
            output(" immJumpTaskShardNum " +", " + "immJumpTaskLoad" +", "+ "migrateLoad" + "\n");
            output( immJumpTaskShardNum +", " + immJumpTaskLoad +", "+ jumpMigrateLoad + "\n");
            if(leftRightNB.equals("left")){
                output(" Tsk" +", " + "SD" + ", " + " <<<<<<<<<" + ", " + "Tsk" + ", " + "SD" + "\n");
                output( immNBTaskID +", " + immigrateNBShardID+", "+ "<<<<<<<<<" +", " + _tid +", "+ emigrateNBShardID+ "\n");
                output(" immNBShardLoad " +", " + "immNBTaskLoad" +", "+ "NB_MigrateLoad" + "\n");
                output( immNBShardLoad +", " + immNBTaskLoad +", "+ NB_MigrateLoad + "\n");
            }
            else if(leftRightNB.equals("right")){
                output(" Tsk" +", " + "SD" + ", " + ">>>>>>>>>" + ", " + "Tsk" + ", " + "SD" + "\n");
                output( _tid +", " + emigrateNBShardID+", "+ ">>>>>>>>>" +", " + immNBTaskID +", "+ immigrateNBShardID+ "\n");
                output(" immNBShardLoad " +", " + "immNBTaskLoad" +", "+ "migrateLoadNB" + "\n");
                output( immNBShardLoad +", " + immNBTaskLoad +", "+ NB_MigrateLoad + "\n");
            }
            else{
                output(" Tsk" +", " + "SD" + ", " + "?????????" + ", " + "Tsk" + ", " + "SD" + "\n");
                output( _tid +", " + emigrateNBShardID+", "+ "?????????" +", " + immNBTaskID +", "+ immigrateNBShardID+ "\n");
                output(" immNBShardLoad " +", " + "immNBTaskLoad" +", "+ "migrateLoadNB" + "\n");
                output( immNBShardLoad +", " + immNBTaskLoad +", "+ NB_MigrateLoad + "\n");
            }*/
        }

        for(int i = 0; i < _shardContainer.size(); i++) {
            Shard shard = _shardContainer.get(i);
            totalNumJoinedT += shard._numTuplesJoined;
            totalNumStoreT += shard._numTuplesStored;
            totalTaskLoad += shard._load;
        }
        if(totalNumJoinedT == 0){
            totalNumJoinedT=1L;
        }

        if(totalNumStoreT == 0){
            totalNumStoreT = 1L;
        }
        double lamda2 =0.0;
        if(joinWinToSec > 0)
        lamda2 = (double) totalNumJoinedT/joinWinToSec;
        output("B1_Addshard= " + B1_Addshard+ ",B1_ReduceSha=" + B1_ReduceSha + "\n");
//        output("------_miu2=" + _miu2 + ",immJumpMiu" + immJumpMiu + ",immNBMiu =" + immNBMiu + ",lamda2 = " + lamda2 + ",immNBTotalJoinT= " + immNBTotalJoinT + ",totalNumStoreT=" + totalNumStoreT+ "\n");

        double ET2 = 0.0;
        if((_miu2 - lamda2) > 0)ET2 = 1/(_miu2 - lamda2);
        else ET2 = 0.001;

        for(int j = 0; j < _shardContainer.size(); j++) {
            //取得shard并采样
            Shard shardItr = _shardContainer.get(j);
            Long shardItrShardId = shardItr._shardID;
            sortStoreTuple = shardItr.SortStoreTuple();
            Double B1C = 0.0;
            double outETETA = 0.0;
            //有连续迁移建议的，计算连续迁移的两个收益
//            if(_continuousM){
                if(emigrateNBShardID.longValue() == shardItrShardId.longValue()){
                    NB_emigrateShardID = shardItr._shardID;
                    NB_immTaskID = immNBTaskID;
                    NB_immigrateShardID = immigrateNBShardID;
                    NB_minRange = shardItr._minRange;
                    NB_maxRange = shardItr._maxRange;
                    //为兜底撤销方案准备参数
                    if(leftRightNB.equals("left")){ NB_cursor = shardItr._minRange; }
                    else{ NB_cursor = shardItr._maxRange; }
                    //①计算连续迁移最佳收益分位点
                    long numStoredMigrationTuple = 0L;
                    long numJoinedMigrationTuple = 0L;
                    int tmpIdx = 0;
                    double immNBLamda=0,lamda2A=0,miu2A=0,immMiuA=0,immNBLamdaA=0;

                    output("totalTaskLoad=" + totalTaskLoad + ",shardItr._load=" + shardItr._load + ",immNBTaskLoad=" + immNBTaskLoad + ",immNBShardStore=" + immNBShardStore + ",immNBShardJoin="+ immNBShardJoin + ",immJumpTaskLoad=" + immJumpTaskLoad+"\n");
                    if(leftRightNB.equals("left")){
                        tmpIdx = 0;
                        double tmp_MAX_NB_profit = Long.MIN_VALUE;
                        long sizeStoreTuple = sortStoreTuple.size();
                        while(tmpIdx < sizeStoreTuple){
                            //假设左迁移至该位置
                            numStoredMigrationTuple = tmpIdx * _storeSampleStep;
                            numJoinedMigrationTuple = tmpIdx * shardItr._numTuplesJoined / sizeStoreTuple;
                            immNBLamda = (double) immNBTotalJoinT/joinWinToSec;
//                            miu2A = _miu2 * totalNumStoreT/(totalNumStoreT - numStoredMigrationTuple);
                            miu2A = estimateMiu(totalTaskLoad, shardItr._numTuplesStored, shardItr._numTuplesJoined, -numStoredMigrationTuple, -numJoinedMigrationTuple, _miu2);
                            lamda2A = (double) (totalNumJoinedT - numJoinedMigrationTuple)/joinWinToSec;
//                            immMiuA = immNBMiu * immNBTotalStoreT/(immNBTotalStoreT + numStoredMigrationTuple);
                            immMiuA = estimateMiu(immNBTaskLoad, immNBShardStore, immNBShardJoin, numStoredMigrationTuple, numJoinedMigrationTuple, immNBMiu);

                            immNBLamdaA = (double) (immNBTotalJoinT + numJoinedMigrationTuple)/joinWinToSec;
                           //比较收益
                            double immET2 = 1/(immNBMiu - immNBLamda);
                            double tmp_NB_profit_Q = 0.0;
                            if(miu2A < lamda2A) miu2A = lamda2A + 100;
                            if(immMiuA < immNBLamdaA)immMiuA = immNBLamdaA + 100;
//                            if((miu2A > lamda2A)&&(immMiuA > immNBLamdaA))
                            tmp_NB_profit_Q = computeLoadBanlanceProfit(ET2, lamda2,
                                    immET2, immNBLamda, miu2A, lamda2A, immMiuA, immNBLamdaA);

                            if(tmp_NB_profit_Q > NB_profit_Q){
                                NB_profit_Q = tmp_NB_profit_Q;
                                NB_cursor = sortStoreTuple.get(tmpIdx);
                            }

                            output("in left: tmpIdx= "+tmpIdx  + ", ET2= " + ET2 + ", immET2= " + immET2 + ", immNBLamda= " + immNBLamda + ", miu2A= " + miu2A
                                    + ",lamda2A=" + lamda2A + ",immMiuA=" + immMiuA + ",immNBLamdaA=" + immNBLamdaA + ",tmp_NB_profit_Q=" + tmp_NB_profit_Q
                                    + ",ET2A=" + 1/(miu2A - lamda2A) + ", immET2A=" + 1/(immMiuA - immNBLamdaA)
                                    + ",totalNumJoinedT=" + totalNumJoinedT+ ",numJoinedMigrationTuple= " + numJoinedMigrationTuple+ ",numStoredMigrationTuple="+numStoredMigrationTuple+"\n");

                            //移动到重复key末尾
                            while(tmpIdx + 1 < sortStoreTuple.size()
                                    && sortStoreTuple.get(tmpIdx + 1).equals(sortStoreTuple.get(tmpIdx))) tmpIdx++;
                            tmpIdx++;
                            ///TODO：LT写的，不知道是干啥的
                            if(tmp_MAX_NB_profit <= tmp_NB_profit_Q) {
                                tmp_MAX_NB_profit = tmp_NB_profit_Q;
                            }else break;
                        }
                    }
                    else{
                        int sizeStoreTuple = sortStoreTuple.size();
                        tmpIdx = sizeStoreTuple - 1;
                        double tmp_MAX_NB_profit = Long.MIN_VALUE;
                        //向右迁移0个元组
                        if(0L > NB_profit){
                            NB_profit = 0l;
                            NB_cursor = shardItr._maxRange;
                        }
                        while(tmpIdx >= 0){
                            //移动到重复key开头
                            while(tmpIdx - 1 >= 0
                                    && sortStoreTuple.get(tmpIdx - 1).equals(sortStoreTuple.get(tmpIdx))) tmpIdx--; ///TODO:这里的等号有问题
                            //假设右移至该位置
                            numStoredMigrationTuple = (sizeStoreTuple - tmpIdx + 1) * _storeSampleStep;
                            numJoinedMigrationTuple = (sizeStoreTuple - tmpIdx + 1) * shardItr._numTuplesJoined / sizeStoreTuple;
                            ///迁移后μ和λ的估算，符合λ/μ与负载呈线性关系，斜率为2E-8
                            immNBLamda = (double) immNBTotalJoinT/joinWinToSec;
//                            miu2A = _miu2 * totalNumStoreT/(totalNumStoreT - numStoredMigrationTuple);
                            miu2A = estimateMiu(totalTaskLoad, shardItr._numTuplesStored, shardItr._numTuplesJoined, -numStoredMigrationTuple, -numJoinedMigrationTuple, _miu2);
                            lamda2A = (double) (totalNumJoinedT - numJoinedMigrationTuple)/joinWinToSec;
//                            immMiuA = immNBMiu * immNBTotalStoreT/(immNBTotalStoreT + numStoredMigrationTuple);
                            immMiuA = estimateMiu(immNBTaskLoad, immNBShardStore, immNBShardJoin, numStoredMigrationTuple, numJoinedMigrationTuple, immNBMiu);
                            immNBLamdaA = (double) (immNBTotalJoinT + numJoinedMigrationTuple)/joinWinToSec;

                            //比较收益
                            double immET2 = 1/(immNBMiu - immNBLamda);
                            double tmp_NB_profit_Q = 0.0;
                            if(miu2A < lamda2A) miu2A = lamda2A + 100;
                            if(immMiuA < immNBLamdaA)immMiuA = immNBLamdaA + 100;
//                            if((miu2A > lamda2A)&&(immMiuA > immNBLamdaA))
                            tmp_NB_profit_Q = computeLoadBanlanceProfit(ET2, lamda2,
                                    immET2, immNBLamda, miu2A, lamda2A, immMiuA, immNBLamdaA);

                            if(tmp_NB_profit_Q > NB_profit_Q){
                                NB_profit_Q = tmp_NB_profit_Q;
                                NB_cursor = sortStoreTuple.get(tmpIdx);
                            }


                            output("in right: tmpIdx= "+tmpIdx  + ", ET2= " + ET2 + ", immET2= " + immET2 + ", immNBLamda= " + immNBLamda + ", miu2A= " + miu2A
                                    + ",lamda2A=" + lamda2A + ",immMiuA=" + immMiuA + ",immNBLamdaA=" + immNBLamdaA + ",tmp_NB_profit_Q=" + tmp_NB_profit_Q
                                    + ",ET2A=" + 1/(miu2A - lamda2A) + ", immET2A=" + 1/(immMiuA - immNBLamdaA)
                                    + ",totalNumJoinedT=" + totalNumJoinedT+ ",numJoinedMigrationTuple= " + numJoinedMigrationTuple+ ",numStoredMigrationTuple="+numStoredMigrationTuple+"\n");


                            tmpIdx--;
                            ///TODO:LT写的，不知道是干啥的
                            if(tmp_MAX_NB_profit <= tmp_NB_profit_Q) {
                                tmp_MAX_NB_profit = tmp_NB_profit_Q;
                            }else break;
                        }
                    }
//                    output("non-entire continuous, NB_profit_Q=" + NB_profit_Q + ", ET2=" + ET2 + ",immET2="+ 1/(immNBMiu - immNBLamda) + ",ET2A=" + 1/(miu2A-lamda2A) + ",immET2A=" + 1/(immMiuA-immNBLamdaA) + "\n");
                    //②计算完整迁移并合并换取奖励方案的收益
                    if(totalNumJoinedT != shardItr._numTuplesJoined){
                        immNBLamda = (double) immNBTotalJoinT/joinWinToSec;
                        miu2A = estimateMiu(totalTaskLoad, shardItr._numTuplesStored, shardItr._numTuplesJoined, -shardItr._numTuplesStored, -shardItr._numTuplesJoined, _miu2);
                        lamda2A = (double) (totalNumJoinedT - shardItr._numTuplesJoined)/joinWinToSec;////
//                        miu2A = (Math.pow ((double) (totalNumStoreT - shardItr._numTuplesStored), pow)/Math.pow(totalNumStoreT, pow)) * _miu2;
//                        miu2A = _miu2 * totalNumStoreT/(totalNumStoreT - shardItr._numTuplesStored);
                        immMiuA = estimateMiu(immNBTaskLoad, immNBShardStore, immNBShardJoin, shardItr._numTuplesStored, shardItr._numTuplesJoined, immNBMiu);
//                        immMiuA = immNBMiu * immNBTotalStoreT/(immNBTotalStoreT + shardItr._numTuplesStored);
                        immNBLamdaA = (double) (immNBTotalJoinT + shardItr._numTuplesJoined)/joinWinToSec;

                        //比较收益
                        double immET2 = 1/(immNBMiu - immNBLamda);
                        double tmp_NB_profit_Q = 0.0;
                        if(miu2A < lamda2A) miu2A = lamda2A + 100;
                        if(immMiuA < immNBLamdaA)immMiuA = immNBLamdaA + 100;
                        tmp_NB_profit_Q = computeLoadBanlanceProfit(ET2, lamda2,
                                    immET2, immNBLamda, miu2A, lamda2A, immMiuA, immNBLamdaA) + B1_ReduceSha;
                        output("in entire continuous, immNBLamda" + "= " + immNBLamda +",ET2=" +ET2+ ",immET2=" + immET2
                                + ",miu2A=" + miu2A + ",lamda2A=" + lamda2A + ",immMiuA=" + immMiuA + ",immNBLamdaA= " + immNBLamdaA + ",tmp_NB_profit_Q="+ tmp_NB_profit_Q
                                + ",ET2A=" + 1/(miu2A - lamda2A) + ", immET2A=" + 1/(immMiuA - immNBLamdaA)
                                + ",totalNumJoinedT=" + totalNumJoinedT + ",shardItr._numTuplesJoined= " + shardItr._numTuplesJoined + ",shardItr._numTuplesStored=" + shardItr._numTuplesStored+"\n");
                        //如果是有效方案分位点设为完全迁移

                        if(tmp_NB_profit_Q >= NB_profit_Q) {
                            NB_profit_Q = tmp_NB_profit_Q;
                            if (leftRightNB.equals("left")) {
                                NB_cursor = shardItr._maxRange;
                            } else {
                                NB_cursor = shardItr._minRange;
                            }
                        }
                        output("entire continuous,tmp_NB_profit_Q= " + tmp_NB_profit_Q + ", ET2=" + ET2 + ",immET2=" + 1/(immNBMiu - immNBLamda) + ",ET2A=" + 1/(miu2A-lamda2A) + ",immETA=" + 1/(immMiuA-immNBLamdaA) + "\n");

                    }
                }
//            }

//            if(_jumpM){
                //计算跳跃迁移的两个收益
                JUMP_immTaskID = immJumpTaskID;
                JUMP_immigrateShardID = 0l;
                //①跳跃完整迁移避免惩罚方案的收益

                double immJumpLamda = (double) immJumpTotalJoinT/joinWinToSec;
                double tmp_JUMP_profit_Q = 0.0;
                double lamdaJump2A = 0.0, miuJump2A = 0.0, immJumpMiuA = 0.0, immJumpLamdaA = 0.0;
                if(totalNumJoinedT != shardItr._numTuplesJoined){
                    miuJump2A = estimateMiu(totalTaskLoad, shardItr._numTuplesStored, shardItr._numTuplesJoined, -shardItr._numTuplesStored, -shardItr._numTuplesJoined, _miu2);
//                    miuJump2A = _miu2 * totalNumStoreT/(totalNumStoreT - shardItr._numTuplesStored);
                    lamdaJump2A = (totalNumJoinedT - shardItr._numTuplesJoined)/joinWinToSec;////
//                    immJumpMiuA = immJumpMiu * immJumpTotalStoredT/(immJumpTotalStoredT + shardItr._numTuplesStored);
                    immJumpMiuA = estimateMiu(immJumpTaskLoad, 0, 0, shardItr._numTuplesStored, shardItr._numTuplesJoined, immJumpMiu);
                    immJumpLamdaA = (double) (immJumpTotalJoinT + shardItr._numTuplesJoined)/joinWinToSec;
                    //比较收益
                    double immET2 = 1/(immJumpMiu - immJumpLamda);

                    if(miuJump2A < lamdaJump2A) miuJump2A = lamdaJump2A + 100;
                    if(immJumpMiuA < immJumpLamdaA)immJumpMiuA = immJumpLamdaA + 100;
                    tmp_JUMP_profit_Q = computeLoadBanlanceProfit(ET2, lamda2,
                                immET2, immJumpLamda, miuJump2A, lamdaJump2A, immJumpMiuA, immJumpLamdaA);
                    output("In jump,totalNumJoinedT=" + totalNumJoinedT + ",shardItr._numTuplesJoined=" + shardItr._numTuplesJoined + ",ET2=" + ET2 + ",immET2="+ immET2
                            + ",miuJump2A=" + miuJump2A + ",lamdaJump2A=" + lamdaJump2A + ",immJumpMiuA=" + immJumpMiuA + ",immJumpLamdaA= " + immJumpLamdaA+",tmp_JUMP_profit_Q="+tmp_JUMP_profit_Q
                            + ",ET2A=" + 1/(miuJump2A - lamdaJump2A) + ", immET2A=" + 1/(immJumpMiuA - immJumpLamdaA)
                            + "\n");

                    if(tmp_JUMP_profit_Q > JUMP_profit_Q){
                        JUMP_profit_Q = tmp_JUMP_profit_Q;
                        JUMP_emigrateShardID = shardItr._shardID;
                        JUMP_minRange = shardItr._minRange;
                        JUMP_cursor = shardItr._maxRange;
                        JUMP_maxRange = shardItr._maxRange;
                    }
                    output("entire jump, JUMP_profit_Q=" + JUMP_profit_Q + "\n");
                }

                //②跳跃迁移最佳收益分位点
                long numStoredMigrationTuple = 0L;
                long numJoinedMigrationTuple = 0L;
                int tmpIdx = 0;
                double tmp_MAX_Jump_profit = Long.MIN_VALUE;
                while(tmpIdx < sortStoreTuple.size()){
                    //假设左迁移至该位置
                    numStoredMigrationTuple = tmpIdx * _storeSampleStep;
                    numJoinedMigrationTuple = tmpIdx * shardItr._numTuplesJoined / sortStoreTuple.size();
                    miuJump2A = estimateMiu(totalTaskLoad, shardItr._numTuplesStored, shardItr._numTuplesJoined, -numStoredMigrationTuple, -numJoinedMigrationTuple, _miu2);
                    lamdaJump2A = (double) (totalNumJoinedT - numJoinedMigrationTuple)/joinWinToSec;
                    immJumpMiuA = estimateMiu(immJumpTaskLoad, 0, 0, numStoredMigrationTuple, numJoinedMigrationTuple, immJumpMiu);

//                    miuJump2A = _miu2 * totalNumStoreT/(totalNumStoreT - numStoredMigrationTuple);

//                    immJumpMiuA = immJumpMiu * immJumpTotalStoredT/(immJumpTotalStoredT + numStoredMigrationTuple);
                    immJumpLamdaA = (double) (immJumpTotalJoinT + numJoinedMigrationTuple)/joinWinToSec;

                    //比较收益
                    double immETJ2 = 1/(immJumpMiu - immJumpLamda);
                    if(miuJump2A < lamdaJump2A) miuJump2A = lamdaJump2A + 100;
                    if(immJumpMiuA < immJumpLamdaA)immJumpMiuA = immJumpLamdaA + 100;
                    tmp_JUMP_profit_Q = computeLoadBanlanceProfit(ET2, lamda2,
                            immETJ2, immJumpLamda, miuJump2A, lamdaJump2A, immJumpMiuA, immJumpLamdaA) + B1_Addshard;

                    output("in non-entire jump: tmpIdx= " + tmpIdx + ", immJumpLamda= " + immJumpLamda + ",ET2=" + ET2+",immETJ2="+immETJ2
                            + ",miuJump2A=" + miuJump2A + ", lamdaJump2A= " + lamdaJump2A + ", immJumpMiuA = "+ immJumpMiuA + ",immJumpLamdaA=" + immJumpLamdaA
                            + ",ET2A=" + 1/(miuJump2A - lamdaJump2A) + ", immET2A=" + 1/(immJumpMiuA - immJumpLamdaA)
                            + ",totalNumJoinedT=" + totalNumJoinedT+ ",numJoinedMigrationTuple= " + numJoinedMigrationTuple
                            + ",numStoredMigrationTuple="+ numStoredMigrationTuple + ",tmp_JUMP_profit_Q=" + tmp_JUMP_profit_Q + "\n");

                    if(tmp_JUMP_profit_Q > JUMP_profit_Q){
                        JUMP_profit_Q = tmp_JUMP_profit_Q;
                        JUMP_emigrateShardID = shardItr._shardID;
                        JUMP_minRange = shardItr._minRange;
                        JUMP_cursor = sortStoreTuple.get(tmpIdx);
                        JUMP_maxRange = shardItr._maxRange;
                    }

                    //移动到重复key末尾  ; TODO:这个不懂~ tmp_MAX_Jump_profit是干啥的？
                    while(tmpIdx + 1 < sortStoreTuple.size()
                            && sortStoreTuple.get(tmpIdx + 1).equals(sortStoreTuple.get(tmpIdx))) tmpIdx++;
                    tmpIdx++;
                    if(tmp_MAX_Jump_profit <= tmp_JUMP_profit_Q) {
                        tmp_MAX_Jump_profit = tmp_JUMP_profit_Q;
                    }else break;
                }
                output("non-entire jump,tmp_JUMP_profit_Q=" + tmp_JUMP_profit_Q + ",JUMP_profit_Q=" + JUMP_profit_Q + "\n");
//            }
        }
        //方案选择
        boolean validMigrateToNeighbor = false, validMigrateJump = false;
        if(_continuousM)validMigrateToNeighbor = NB_profit_Q >= 0.0;
        if(_jumpM)validMigrateJump = JUMP_profit_Q > 0.0 &&  immJumpTaskShardNum < _numShard;
        if(validMigrateToNeighbor && validMigrateJump) { //选择最佳方案
            if(JUMP_profit_Q > NB_profit_Q)
                modifyMigrateRegion("jump", JUMP_emigrateShardID, JUMP_immTaskID, JUMP_immigrateShardID, JUMP_minRange, JUMP_cursor, JUMP_maxRange, collector);
            else
                modifyMigrateRegion(leftRightNB, NB_emigrateShardID, NB_immTaskID, NB_immigrateShardID, NB_minRange, NB_cursor, NB_maxRange, collector);
        }
        else if(validMigrateToNeighbor && !validMigrateJump) //连续方案无效，只有跳跃迁移方案可选
            modifyMigrateRegion(leftRightNB, NB_emigrateShardID, NB_immTaskID, NB_immigrateShardID, NB_minRange, NB_cursor, NB_maxRange, collector);
        else if(!validMigrateToNeighbor && validMigrateJump) //跳跃方案无效，只有连续方案可选
            modifyMigrateRegion("jump", JUMP_emigrateShardID, JUMP_immTaskID, JUMP_immigrateShardID, JUMP_minRange, JUMP_cursor, JUMP_maxRange, collector);
        else { //无方案可选，进行无影响连续迁移撤销
//            if(leftRightNB.equals("left")){ NB_cursor = _shardContainer.get(0)._minRange; }
//            else{ NB_cursor = _shardContainer.get(0)._maxRange; }
            modifyMigrateRegion(leftRightNB, NB_emigrateShardID, NB_immTaskID, NB_immigrateShardID, NB_minRange, NB_cursor, NB_maxRange, collector);
        }

        /*DEBUG_LOG*/{
            output("-----------migrate(): Compute Profit-------------" + "\n");
            output(" NB_profit " +", " + "JUMP_profit" + ", JUMP_profit_Q" + ", NB_profit_Q" + ", "+ "choose" + "\n");

            String chooseString = "null";
            if(validMigrateToNeighbor && validMigrateJump) //选择最佳方案
                if(JUMP_profit_Q >= NB_profit_Q) chooseString = "jump";
                else chooseString = leftRightNB;
            else if(validMigrateToNeighbor && !validMigrateJump) //连续方案无效，只有跳跃迁移方案可选
                chooseString = "jump";
            else if(!validMigrateToNeighbor && validMigrateJump) //跳跃方案无效，只有连续方案可选
                chooseString = leftRightNB;
            else //无方案可选，进行无影响迁移撤销
                chooseString = "recall";

            output( NB_profit +", " + JUMP_profit +", " + JUMP_profit_Q + ", " + NB_profit_Q + chooseString + "\n");
            output( "immJumpTaskShardNum" + "\n");
            output(  immJumpTaskShardNum + "\n");

            output("JUMP_profit_Q=" + JUMP_profit_Q + ", NB_profit_Q=" + NB_profit_Q + "\n");
        }

        output("the time of migration and computing benefit (ms):" + (System.currentTimeMillis()-ts) + ",CurrTime=" + System.currentTimeMillis() + "\n");
    }

    private void modifyMigrateRegion(String leftRightOrJump, Long emigrateShardID, Long immTaskID, Long immigrateShardID, Long minRange, Long cursorL, Long maxRange, BasicOutputCollector collector){ ////这个得在上层函数。。
//        output("in the modifyMigrateRegion." + ",leftRightOrJump=" + leftRightOrJump + ",immTaskID.intValue()=" + immTaskID.intValue() + "\n");
        /*DEBUG_LOG*/
        {
            output("-----------modifyMigrateRegion(): Decision-------------" + "\n");
        }
        StringBuilder debugstrB = new StringBuilder();
        for(int m = 0; m < _shardContainer.size(); m++){
            long DebugBeforeImmTuple = _shardContainer.get(m)._numTuplesStored;
            long DebugCountStoreTuple = _shardContainer.get(m).countTuples();
            debugstrB.append("_numTuplesStored: " + DebugBeforeImmTuple + ", DebugCountStoreTuple:" + DebugCountStoreTuple + ",shardID:" + _shardContainer.get(m)._shardID + "\n");

            if(DebugBeforeImmTuple != DebugCountStoreTuple)debugstrB.append("DebugBeforeImmTuple != DebugCountStoreTuple" + "\n");
//            output("m="+m+",_shardContainer.size()="+_shardContainer.size()+"\n");
            if(emigrateShardID == _shardContainer.get(m)._shardID){
                if(leftRightOrJump.equals("left")){////向左迁移，reshuffler收到的是modify，就只检查minRange和cursor就够了，就能判定是向左还是向右，这里为了节省一个字段少了个向左还是向右的判定。
                    /*DEBUG_LOG*/
                    {
                        output(" Tsk" + ", " + "SD" + ", " + " <<<<<<<<<" + ", " + "Tsk" + ", " + "SD" + "\n");
                        output(immTaskID + ", " + immigrateShardID + ", " + "<<<<<<<<<" + ", " + _tid + ", " + emigrateShardID + "\n");
                        output(" [" + minRange + ", " + cursorL + ")" + " <<<<<<<<<"
                                + " [" + cursorL + ", " + maxRange + ")" + "\n");
                        output("-----------modifyMigrateRegion(): Before-------------" + "\n");
                        output(" @SD: " + _shardContainer.get(m)._shardID + "\n");
                        output(" _minRange" +", " + "_maxRange" + ", " + "_numTuplesStored" + "\n");
                        output( _shardContainer.get(m)._minRange +", "  + _shardContainer.get(m)._maxRange +", "  + _shardContainer.get(m)._numTuplesStored + "\n");
                    }
                    _shardContainer.get(m)._minRange = cursorL;
                    /////怎么样通知迁入的task修改maxRange？ 上面这样确定把minRange改了？  这里能随便emit吗？  type", "relation", "emiShardID", "immTaskID", "immShardID", "minRange", "cursor", "maxRange", "numMigrateTuple"
                    collector.emitDirect(immTaskID.intValue(), JOINER_TO_JOINER_MODIFY_STREAM_ID, new Values("modify", immigrateShardID, leftRightOrJump, Long.MIN_VALUE, cursorL, Long.MIN_VALUE));

                    long numMigrateTuple = _shardContainer.get(m).immigrateTuples(immTaskID.intValue(), immigrateShardID, "left", cursorL, collector);
                    long numMigrateJoinedTuple = _shardContainer.get(m).subJoinedTupleNum(immTaskID.intValue(), immigrateShardID, numMigrateTuple, collector);
                    long currTime = System.currentTimeMillis();
                    /*DEBUG_LOG*/{
                        output("currTime:" + currTime + "\n");
                    }
                    collector.emit(JOINER_TO_RESHUFFLER_STREAM_ID, new Values("modify", _taskRel, emigrateShardID, immTaskID, immigrateShardID, Long.MIN_VALUE, cursorL, Long.MIN_VALUE, numMigrateTuple, currTime));//minLoadId还可能是迁入的哪个邻居的id。

                    /*DEBUG_LOG*/
                    {
                        output("-----------modifyMigrateRegion(): After-------------" + "\n");
                        output(" @SD: " + _shardContainer.get(m)._shardID + "\n");
                        output(" _minRange" +", " + "_maxRange" + ", " + "_numTuplesStored" + ", " + "numMigrateTuple"+ "\n");
                        output( _shardContainer.get(m)._minRange +", "  + _shardContainer.get(m)._maxRange +", "  + _shardContainer.get(m)._numTuplesStored +", "  + numMigrateTuple + "\n");
                    }
                    if(cursorL.longValue() == maxRange.longValue()){///如果把整个shard都迁移走了，删除迁出shard。
                        output("To left, because cursorL == maxRange, deleteShard!" + "\n");
                        deleteShard(emigrateShardID);
                    }
                }
                else if(leftRightOrJump.equals("right")){//// 向右
                    /*DEBUG_LOG*/
                    {
                        output(" Tsk" +", " + "SD" + ", " + ">>>>>>>>>" + ", " + "Tsk" + ", " + "SD" + "\n");
                        output( _tid +", " + emigrateShardID+", "+ ">>>>>>>>>" +", " + immTaskID +", "+ immigrateShardID+ "\n");
                        output(" [" + minRange + ", " + cursorL + ")" + " >>>>>>>>> "
                                + " [" + cursorL + ", " + maxRange + ")" + "\n");
                        output("-----------modifyMigrateRegion(): Before-------------" + "\n");
                        output(" @SD: " + _shardContainer.get(m)._shardID + "\n");
                        output(" _minRange" +", " + "_maxRange" + ", " + "_numTuplesStored" + "\n");
                        output( _shardContainer.get(m)._minRange +", "  + _shardContainer.get(m)._maxRange +", "  + _shardContainer.get(m)._numTuplesStored + "\n");
                    }
                    _shardContainer.get(m)._maxRange = cursorL;
                    collector.emitDirect(immTaskID.intValue(), JOINER_TO_JOINER_MODIFY_STREAM_ID, new Values("modify", immigrateShardID, leftRightOrJump, Long.MAX_VALUE, cursorL, Long.MAX_VALUE));
                    long numMigrateTuple = _shardContainer.get(m).immigrateTuples(immTaskID.intValue(), immigrateShardID, "right", cursorL, collector);
                    long numMigrateJoinedTuple = _shardContainer.get(m).subJoinedTupleNum(immTaskID.intValue(), immigrateShardID, numMigrateTuple, collector);
                    long currTime1 = System.currentTimeMillis();
                    /*DEBUG_LOG*/{
                        output("currTime:" + currTime1 + "\n");
                    }
                    collector.emit(JOINER_TO_RESHUFFLER_STREAM_ID, new Values("modify", _taskRel, emigrateShardID, immTaskID, immigrateShardID, Long.MAX_VALUE, cursorL, Long.MAX_VALUE, numMigrateTuple, currTime1));//minLoadId还可能是迁入的哪个邻居的id。

                    /*DEBUG_LOG*/
                    {
                        output("-----------modifyMigrateRegion(): After-------------" + "\n");
                        output(" @SD: " + _shardContainer.get(m)._shardID + "\n");
                        output(" _minRange" +", " + "_maxRange" + ", " + "_numTuplesStored" + ", " + "numMigrateTuple"+ ", "+ "numMigrateJoinedTuple"+ "\n");
                        output( _shardContainer.get(m)._minRange +", "  + _shardContainer.get(m)._maxRange +", "  + _shardContainer.get(m)._numTuplesStored +", "  + numMigrateTuple + ", "+ numMigrateJoinedTuple + "\n");
                    }
                    if(cursorL.longValue() == minRange.longValue())///如果把整个shard都迁移走了，删除迁出shard。
                    {
                        output("To right, because cursorL == minRange, deleteShard!" + "\n");
                        deleteShard(emigrateShardID);
                    }
                }
                else if(leftRightOrJump.equals("jump")){
                    Long ts = System.currentTimeMillis();
                    Long jumpImmShardID = immTaskID<<52 | (ts/1000)<<20 | ((long)((1L<<20)*Math.random()) & 0xFFFFF);//"type", "immShardID", "leftRightOrJump", "minRange", "cursor", "immTaskID"
                    /*DEBUG_LOG*/
                    {
                        output(" Tsk" +", " + "SD" + ", " + " <<<~~~<<<" + ", " + "Tsk" + ", " + "SD" + "\n");
                        output( immTaskID +", " + jumpImmShardID +", "+ "<<<~~~<<<" +", " + _tid +", "+ emigrateShardID+ "\n");
                        output(" [" + minRange + ", " + cursorL + ")" + " <<<<<<<<<"
                                + " [" + cursorL + ", " + maxRange + ")" + "\n");
                        output("-----------modifyMigrateRegion(): Before-------------" + "\n");
                        output(" @SD: " + _shardContainer.get(m)._shardID + "\n");
                        output(" _minRange" +", " + "_maxRange" + ", " + "_numTuplesStored" + "\n");
                        output( _shardContainer.get(m)._minRange +", "  + _shardContainer.get(m)._maxRange +", "  + _shardContainer.get(m)._numTuplesStored + "\n");
                    }
                    _shardContainer.get(m)._minRange = cursorL;
                    long numMigrateTuple = 0l;
                    long numMigrateJoinedTuple = 0l;
//                    if(minRange < cursorL){
                        collector.emitDirect(immTaskID.intValue(), JOINER_TO_JOINER_MODIFY_STREAM_ID, new Values("shard", jumpImmShardID, leftRightOrJump, minRange, cursorL, immTaskID));
                        numMigrateTuple =  _shardContainer.get(m).immigrateTuples(immTaskID.intValue(), jumpImmShardID, "left", cursorL, collector);
                        numMigrateJoinedTuple = _shardContainer.get(m).subJoinedTupleNum(immTaskID.intValue(), jumpImmShardID, numMigrateTuple, collector);
                    long currTime2 = System.currentTimeMillis();
                    /*DEBUG_LOG*/{
                        output("currTime:" + currTime2 + "\n");
                    }
                    collector.emit(JOINER_TO_RESHUFFLER_STREAM_ID, new Values("shard", _taskRel, emigrateShardID, immTaskID, jumpImmShardID, minRange, cursorL, maxRange, numMigrateTuple, currTime2));//minLoadId还可能是迁入的哪个邻居的id。
                    /*DEBUG_LOG*/ //type", "relation", "emiShardID", "immTaskID", "immShardID", "minRange", "cursor", "maxRange", "numMigrateTuple"
                    {
                        output("-----------modifyMigrateRegion(): After-------------" + "\n");
                        output(" @SD: " + _shardContainer.get(m)._shardID + "\n");
                        output(" _minRange" +", " + "_maxRange" + ", " + "_numTuplesStored" + ", " + "numMigrateTuple"+ "numMigrateJoinedTuple" + "\n");
                        output( _shardContainer.get(m)._minRange +", "  + _shardContainer.get(m)._maxRange +", "  + _shardContainer.get(m)._numTuplesStored +", "  + numMigrateTuple +", " + numMigrateJoinedTuple + "\n");
                    }
                    if(cursorL.longValue() == maxRange.longValue())///如果把整个shard都迁移走了，删除迁出shard。
                    {
                        output("Jump, because cursorL == maxRange, deleteShard!" + "\n");
                        deleteShard(emigrateShardID);
                    }
                }
                else{
                    //////*DEBUG_LOG*/
                    {
                        output(" Tsk" + ", " + "SD" + ", " + "?????????" + ", " + "Tsk" + ", " + "SD" + "\n");
                        output(immTaskID + ", " + immigrateShardID + ", " + "?????????" + ", " + _tid + ", " + emigrateShardID + "\n");
                        output(" [" + minRange + ", " + cursorL + ")" + "?????????"
                                + " [" + cursorL + ", " + maxRange + ")" + "\n");
                    }
                }
                break;
            }
        }
        output("\n" + debugstrB.toString() + "\n");
    }

    private long JumpPenalty(Shard emitShard){ ////SelfLoad * （e1 + e2)/(minRagen + cursor)
        double penaltyLoad = 0.0;
        if(emitShard._maxRange > emitShard._minRange){
            penaltyLoad = (double) ((_e1 + _e2)/(emitShard._maxRange - emitShard._minRange)) * emitShard._numTuplesStored * emitShard._numTuplesJoined;
        }
        return (long) penaltyLoad;
    }

    ////TODO: 在joiner， reshuffle, 和monitor都要删除。
    private void deleteShard(long shardID){
        for(int i = 0; i < _shardContainer.size(); i++){
            if(_shardContainer.get(i)._shardID == shardID){
                _shardContainer.remove(i);
                break;
            }
        }
    }

    private boolean checkTimeStamp(long ts){
        if(ts < _lastTime - 100) return false;/////是迁移来的元组
        else if(ts <= _lastTime + 100) return true;  ///是新来的未处理的元组
        else {
            _lastTime = System.currentTimeMillis();
            if(ts <= _lastTime - 100)return false;
            else if(ts <= _lastTime + 100) return true;
            else return true;
        }
    }

    private double estimateMiu(long taskLoad, long shardStoreT, long shardJoinedT, long numStoredMigrationTuple, long numJoinedMigrationTuple, double miu) {
        double miuA = 0.0;
        long shardload = shardStoreT*shardJoinedT;
        long shardLoadA = (shardStoreT + numStoredMigrationTuple) * (shardJoinedT + numJoinedMigrationTuple);
        long totalLoadA = taskLoad - shardload + shardLoadA;

        double shardMiu = 0.0, shardMiuA = 0.0;
        if(totalLoadA ==0) totalLoadA = 1;
        if(shardStoreT != 0)shardMiu = (shardload*_consProcess/totalLoadA)/shardStoreT;
//        double shardMiuA = (shardLoadA)*_consProcess/totalLoadA;
        double miuP = miu * taskLoad/totalLoadA;
        if(shardStoreT + numStoredMigrationTuple > 0)
        shardMiuA = (shardLoadA * _consProcess/totalLoadA)/(shardStoreT + numStoredMigrationTuple);
        miuA = miuP - (shardMiu - shardMiuA);
        return  miuA;
    }

/*    private double estimateImmMiu(long taskLoad, long shardStoreT, long shardJoinedT, long numStoredMigrationTuple, long numJoinedMigrationTuple, long shardload, double miu) {
        double miuA = 0.0;
        shardJoinedT = shardStoreT * shardJoinedT;
        double immShardLoadA = (shardStoreT + numStoredMigrationTuple) * (shardJoinedT + numJoinedMigrationTuple);
        double immDeltaShardLoad = immShardLoadA - shardload;
        double immTotalLoadA = taskLoad + immShardLoadA - shardload;
        double immShardMiu = (shardStoreT * shardJoinedT)*_consProcess/taskLoad;///immNBTaskLoad;
        double immShardMiuA = immShardLoadA*_consProcess/immTotalLoadA;
        miuA = miu - immShardMiu + immShardMiuA;
        return  miuA;
    }*/

}
