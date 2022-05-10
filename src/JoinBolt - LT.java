package soj.biclique.bolt;

import java.text.DecimalFormat;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

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
    private static final List<String> TO_RESHUFFLER_SCHEMA = ImmutableList.of( "type", "relation", "emiShardID", "immTaskID", "immShardID", "minRange", "cursor", "maxRange", "numMigrateTuple");///"shard", _taskRel, emigrateShardID, immTaskID, immigrateShardID, minRange, tmpCursor, maxRange
    private static final List<String> TO_MONITOR_SCHEMA = ImmutableList.of("type", "taskrel", "timestamp", "statistics");
//    private static final List<String> TO_MONITOR_SCHEMA = ImmutableList.of("type", "relation", "key", "target");
    private static final long PROFILE_REPORT_PERIOD_IN_SEC = 1;
    private static final int BYTES_PER_TUPLE_R = 64;
    private static final int BYTES_PER_TUPLE_S = 56;
    private static final int BYTES_PER_MB = 1024 * 1024;

    private static final Logger LOG = getLogger(JoinBolt.class);
    private String _taskRel;

    private double _interval;
    private long _thisWinSize;
    private long _e1, _e2;

    public JoinBolt(String relation, double interval, boolean win, long winsize, long e1, long e2, int numShard) {
        super();
        _taskRel = relation;
        _interval = interval;
        _window = win;
        _thisWinSize = winsize;
        _e1 = e1;
        _e2 = e2;
        _numShard = numShard;

        checkState(_taskRel.equals("R") || _taskRel.equals("S"), "Unknown relation: "
                + _taskRel);
    }

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
    private long _lastTuplesJoined;
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
    private long _lastTupleJoined, _tuplesJoined;
    private int _storeSampleStep;//分别为连接元组和存储元组采样排序的抽样比。其中_joinSampleStep为存储的时候就是抽样存储的，所以不用在此二次抽样。_joinSampleStep,
    private double _alpha1, _alpha2;
    private int _numShard;

    List<Shard> _shardContainer = new ArrayList<Shard>(1);
    @Override
    public void prepare(Map conf, TopologyContext context) {
        _tid = context.getThisTaskId();
        _context = context;
        String prefix = "joiner_" + _taskRel.toLowerCase() + _tid;
        _output = new FileWriter("/home/shuiyinyu/join/tmpresult-fastjoin", prefix, "csv");///.setFlushSize(1);//..setPrintStream(System.out);

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
        _lastTuplesJoined = 0;
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
        _lastTupleJoined = 0l;
//        _joinSampleStep = 30;
        _storeSampleStep = 30;
        _tuplesJoined= 0;
        _alpha1 = 800;
        _alpha2 = 0.05;
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
                    if(belongShard(shard._minRange,shard._maxRange,rel,key))shard.store(tuple);
                }

                else {
                    if(belongShardForJoin(shard._minRange,shard._maxRange,rel,key)){
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
            /*DEBUG_LOG*/{
                output("  [ Receive & add Shard ]  " + "\n");
                output("from @tsk-SD: " +tuple.getSourceTask() + "-" + shardID + "\n");
                output(" minRange " +", " + "maxRange" +", "+ "owner" + "\n");
                output( minRange +", " + cursor +", "+ owner + "\n");
                output("  [-]  " + "\n");
            }
        }
        else if(type.equals("migration")){ ////开始迁移
            ////计算收益，操作有，给存储元组和连接元组排序，收益计算。 "type", "minLoadId", "minLoad", "nbIdsLoads"
            Long minLoadId = tuple.getLongByField("minLoadId");
            String minLoad = tuple.getStringByField("minLoad");
            String nbLowShard = tuple.getStringByField("nbLowShard");////???？？？不一定对

            /*DEBUG_LOG*/{
                output("  [ Start migration ]  " + "\n");
            }
            migrate(minLoadId, minLoad, nbLowShard, collector);
            /*DEBUG_LOG*/{
                output("  [-]  " + "\n");
            }
        }
        else if(type.equals("modify")){  ///"type", "immShardID", "leftRightOrJump", "minRange", "maxRange"
            Long shardID = tuple.getLongByField("immShardID");
            String leftOrRight = tuple.getStringByField("leftRightOrJump");
            Long cursor = tuple.getLongByField("cursor");
            /*DEBUG_LOG*/{
                output("  [ Receive Modify ]  " + "\n");
                output("from @tsk: " +tuple.getSourceTask() + "\n");
                output(" shardID " +", " +" leftOrRight " +", " + "cursor"  + "\n");
                output( shardID +", " + leftOrRight +", " + cursor + "\n");
                output("  [-]  " + "\n");
            }
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
                            output(" [" + oldMin + ", " + oldMax + ")" +"-> " + " [" + newMin + ", " + newMax + ")"  + "\n");
                            output( "modifyShard()  " + debug_modifyShard + "\n");
                        }
                    }else if(leftOrRight.equals("right")){ ///"modify", immigrateShardID, leftRightOrJump, "null", tmpCursor, "null"
                        oldMin = _shardContainer.get(i)._minRange ;
                        oldMax = _shardContainer.get(i)._maxRange ;
                        debug_modifyShard = _shardContainer.get(i).modifyShard(leftOrRight, cursor, 0l);
                        newMin = _shardContainer.get(i)._minRange ;
                        newMax = _shardContainer.get(i)._maxRange ;
                        /*DEBUG_LOG*/{
                            output(" [" + oldMin + ", " + oldMax + ")" +"-> " + " [" + newMin + ", " + newMax + ")"  + "\n");
                            output( "modifyShard()  " + debug_modifyShard + "\n");
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
                output("sha._shardID= " + sha._shardID + ",sha._minRange=" + sha._minRange + ",sha._maxRange=" + sha._maxRange + "\n");
                _shardContainer.add(sha);
                /*DEBUG_LOG*/{
                    output("  [ InitialShard ]  @SD:  " +sha._shardID+ " [" + sha._minRange + ", " + sha._maxRange + ")"+  "\n");
                }
            }
        }

        if (isTimeToOutputProfile()) {
            long interval_currts = _stopwatch.elapsed(MILLISECONDS);
            StringBuilder sb = new StringBuilder();

            /*DEBUG_LOG*/{
                output("  [ Load Report ]  @sec " +_stopwatch.elapsed(SECONDS) +  "\n");
            }
            for(int i = 0; i < _shardContainer.size(); i++){
                Shard shard = _shardContainer.get(i);
                if(i > 0){
                    sb.append(";");
                }
                sb.append(shard.encodeShardInJoiner());

                /*DEBUG_LOG*/{
                    output("-item- " + shard.encodeShardInJoiner() +  "\n");
                }
            }

            String shardStatistics = sb.toString();
            if (_taskRel.equals("R"))
                collector.emit(MONITOR_R_STREAM_ID, new Values("recordSignal-bolt", "R", interval_currts, shardStatistics));
            else if (_taskRel.equals("S"))
                collector.emit(MONITOR_S_STREAM_ID, new Values("recordSignal-bolt", "S", interval_currts, shardStatistics));
            long moment = _stopwatch.elapsed(SECONDS);
            long joinTimes = _joinedTime - _lastJoinedTime;
            long processingDuration1 = 1;
            long currTuplesJoined = _tuplesJoined - _lastTupleJoined;
            collector.emit(JOINER_TO_POST_STREAM_ID, new Values(moment, getShardStoredT(_shardContainer), joinTimes, processingDuration1, _latency, _migrationTimes, currTuplesJoined, _migrationTuples, _load));
            _latency = 0;
//            _tuplesJoinBefore = 0;
            _lastTupleJoined = _tuplesJoined;

            /*DEBUG_LOG*/{
                output(" interval_currts " +", " +" _lastTupleJoined "  + "\n");
                output( interval_currts +", " + _lastTupleJoined + "\n");
                output("  [-]  " + "\n");
            }

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

/*    private long getShardJoinedT(List<Shard> shardContainer){
        long joinedTuple = 0l;
        for(int i = 0; i < shardContainer.size(); i++){
            Shard sha = shardContainer.get(i);
            joinedTuple = joinedTuple + sha.getNumJoined();
        }
        return joinedTuple;
    }*/

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
//
//    private void migrate(Long minId , String minLoad, String nbLowShard, BasicOutputCollector collector){  //应该返回迁入joiner的ID，这里还要给出迁移之后的minRa和maxRange
////        ArrayList<Long> results = new ArrayList<>(4);
//
//        Long maxJumpProfit = -1l, maxJumpEmiShardID = 0l, jumpEmiMinRange = 0l, nbEmitMinRange = 0l, nbEmitMaxRange = 0l, immigrateMinRange = 0l, immigrateMaxRange = 0l;
//        Long jumpEmitTaskID = 0l, tempMigLoad = 0l, tempNBMigLoad = 0l, tempDeltaLoad = Long.MAX_VALUE, maxJumpCursorSKey = 0l, nbCursorSKey = 0l;
//        int tmpJumpCursorS = 0, tmpNBCursorS = 0, tmpNBCursorJ = 0, jumpCursorS = 0;
//        double profitEntireLeftNBSha = 0.0, profitEntireRightNBSha = 0.0, profitLeftNBLoad = 0.0, profitRightNBLoad = 0.0, penaltyLoad = 0.0, entireRewards = 0.0;
//        List<Long> sortStoreTuple =  new ArrayList<>();
////        List<Long> sortJoinTuple = new ArrayList<>();
//        Long taskLoadSelf = getTaskLoad();
//
//        String []minTaskLoad = minLoad.split(",");
//        String []nbLowSha = nbLowShard.split(",");
//
//        Long immJumpTaskID = Long.parseLong(minTaskLoad[0]);
//        Long immJumpTaskShardNum = Long.parseLong(minTaskLoad[1]);
//        Long immJumpTaskLoad = Long.parseLong(minTaskLoad[2]);
//        Long jumpMigrateLoad = (taskLoadSelf - immJumpTaskLoad) / 2;
//
//        Long immigrateNBShardID = Long.parseLong(nbLowSha[0]);
//        Long immMinRangeNB = Long.parseLong(nbLowSha[1]);
//        Long immMaxRangeNB = Long.parseLong(nbLowSha[2]);
//
//        Long immNBTupleStoreNum = Long.parseLong(nbLowSha[4]);
//        Long immNBTupleJoinNum = Long.parseLong(nbLowSha[5]);
//        Long emTaskIdNB = Long.parseLong(nbLowSha[6]);
//        Long emigrateNBShardID = Long.parseLong(nbLowSha[7]);
//        String leftRightNB = String.valueOf(nbLowSha[8]);
//        Long immNBTaskID = Long.parseLong(nbLowSha[9]);
//        Long immNBTaskLoad = Long.parseLong(nbLowSha[10]);///numTmTupleStoreNB * nuupleJoinNB;///
//        Long NB_MigrateLoad = (taskLoadSelf - immNBTaskLoad)/2;
//        Long immNBShardLoad = immNBTupleStoreNum * immNBTupleJoinNum;
//
//        /*DEBUG_LOG*/{
//            output("-----------migrate(): Recv Suggestion-------------" + "\n");
//            output(" Tsk" +", " + "SD" + ", " + " <<<~~~<<<" + ", " + "Tsk" + ", " + "SD" + "\n");
//            output( immJumpTaskID +", " + "?" +", "+ "<<<~~~<<<" +", " + _tid +", "+ "?"+ "\n");
//            output(" immJumpTaskShardNum " +", " + "immJumpTaskLoad" +", "+ "migrateLoad" + "\n");
//            output( immJumpTaskShardNum +", " + immJumpTaskLoad +", "+ jumpMigrateLoad + "\n");
//            if(leftRightNB.equals("left")){
//            output(" Tsk" +", " + "SD" + ", " + " <<<<<<<<<" + ", " + "Tsk" + ", " + "SD" + "\n");
//            output( immNBTaskID +", " + immigrateNBShardID+", "+ "<<<<<<<<<" +", " + _tid +", "+ emigrateNBShardID+ "\n");
//            output(" immNBShardLoad " +", " + "immNBTaskLoad" +", "+ "NB_MigrateLoad" + "\n");
//            output( immNBShardLoad +", " + immNBTaskLoad +", "+ NB_MigrateLoad + "\n");
//            }
//            else if(leftRightNB.equals("right")){
//                output(" Tsk" +", " + "SD" + ", " + ">>>>>>>>>" + ", " + "Tsk" + ", " + "SD" + "\n");
//                output( _tid +", " + emigrateNBShardID+", "+ ">>>>>>>>>" +", " + immNBTaskID +", "+ immigrateNBShardID+ "\n");
//                output(" immNBShardLoad " +", " + "immNBTaskLoad" +", "+ "migrateLoadNB" + "\n");
//                output( immNBShardLoad +", " + immNBTaskLoad +", "+ NB_MigrateLoad + "\n");
//            }
//            else{
//                output(" Tsk" +", " + "SD" + ", " + "?????????" + ", " + "Tsk" + ", " + "SD" + "\n");
//                output( _tid +", " + emigrateNBShardID+", "+ "?????????" +", " + immNBTaskID +", "+ immigrateNBShardID+ "\n");
//                output(" immNBShardLoad " +", " + "immNBTaskLoad" +", "+ "migrateLoadNB" + "\n");
//                output( immNBShardLoad +", " + immNBTaskLoad +", "+ NB_MigrateLoad + "\n");
//            }
//        }
//
//        for(int j = 0; j < _shardContainer.size(); j++) {
//            ///计算连续迁移的收益 TODO：迁出节点找到的邻居shard在本地。要在monitor中考虑。
//            Shard emigraShard = _shardContainer.get(j);
//            Long emigraShardID = emigraShard._shardID;
//            sortStoreTuple = emigraShard.SortStoreTuple();
////            sortJoinTuple = emigraShard.SortJoinTuple();
//
//            ////判断迁出shard是否在是_shardContainer中的某个节点
//            Long tmpKey = 0L;
//            if((emigrateNBShardID.longValue() == emigraShardID.longValue()) && leftRightNB.equals("left")){///说明是邻居，向左迁移
//                tmpNBCursorS = 0;
//                tmpNBCursorJ = 0;
//                Long emitNBShardLoad = emigraShard._numTuplesStored * emigraShard._numTuplesJoined;//TODO:0103 ._numSampleJoinTuples * _joinSampleStep;
//                tmpKey = emigraShard._minRange;
//                long joinedTupleStep = (long) (1.0/sortStoreTuple.size() * emigraShard._numTuplesJoined);///emigraShard.getRangeCountTuple(emigraShard._minRange, tmpKey);////TODO:0103膝盖
//                long immTupleJoinedNum = 0l;
//                while(tempNBMigLoad < NB_MigrateLoad && tmpNBCursorS < sortStoreTuple.size()){
//                    tmpKey = sortStoreTuple.get(tmpNBCursorS);
//                    immTupleJoinedNum = immTupleJoinedNum + joinedTupleStep;///emigraShard.getRangeCountTuple(emigraShard._minRange, tmpKey);////TODO:0103膝盖
//                    tempNBMigLoad = (immNBTupleStoreNum + tmpNBCursorS * _storeSampleStep) * (immNBTupleJoinNum + immTupleJoinedNum)  - immNBShardLoad; ////TODO:0103膝盖
//                    tmpNBCursorS++;
//                }
//                nbEmitMinRange = emigraShard._minRange;
//                nbEmitMaxRange = emigraShard._maxRange;
//                nbCursorSKey = tmpKey;
//                profitLeftNBLoad = NB_MigrateLoad - Math.abs(NB_MigrateLoad - 2 * tempNBMigLoad);
//                entireRewards = ((double) tmpNBCursorS / (double) sortStoreTuple.size()) * emitNBShardLoad * _alpha2;
//                profitEntireLeftNBSha = Math.abs(NB_MigrateLoad - Math.abs(NB_MigrateLoad - 2 * emitNBShardLoad)) + entireRewards;
//
////                profitEntireNBSha = () + (1-(double)tmpNBCursorS/sortStoreTuple.size()) * emitNBShardLoad * _alpha2;
//            }else if((emigrateNBShardID.longValue() == emigraShardID.longValue()) && leftRightNB.equals("right")) {///说明是邻居，向右迁移
//                tmpNBCursorS = sortStoreTuple.size();
////                tmpNBCursorJ = sortJoinTuple.size();
//                Long emitNBShardLoad = emigraShard._numTuplesStored * emigraShard._numTuplesJoined;////TODO:._numSampleJoinTuples * _joinSampleStep;
//                tmpKey = emigraShard._maxRange;
//                tempNBMigLoad = 0l;
//                long joinedTupleStep = (long) (1.0/sortStoreTuple.size() * emigraShard._numTuplesJoined);///emigraShard.getRangeCountTuple(emigraShard._minRange, tmpKey);////TODO:0103膝盖
//                long immTupleJoinedNum = 0l;
//                while (tempNBMigLoad < NB_MigrateLoad && tmpNBCursorS > 0) {
//                    tmpNBCursorS--;
//                    tmpKey = sortStoreTuple.get(tmpNBCursorS);
//                    immTupleJoinedNum = immTupleJoinedNum + joinedTupleStep;
////                    long immTupleJoinedNum = emigraShard.getRangeCountTuple(tmpKey, emigraShard._maxRange);////TODO:0103膝盖
///*                    for (; tmpNBCursorJ > 0; tmpNBCursorJ--) {
//                        if (sortJoinTuple.get(tmpNBCursorJ - 1) <= tmpKey) break;
//                    }*/
//                    tempNBMigLoad = (immNBTupleStoreNum + (sortStoreTuple.size() - tmpNBCursorS) * _storeSampleStep) *
//                            (immNBTupleJoinNum + immTupleJoinedNum) - immNBShardLoad;
//                }
//                nbEmitMinRange = emigraShard._minRange;
//                nbEmitMaxRange = emigraShard._maxRange;
//                nbCursorSKey = tmpKey;
//                profitRightNBLoad = NB_MigrateLoad - Math.abs(NB_MigrateLoad - 2 * tempNBMigLoad);
//                entireRewards = (1 - (double) tmpNBCursorS / (double) sortStoreTuple.size()) * emitNBShardLoad * _alpha2;
//                ///TODO:如果是整个shard迁移，可以合并shard，给奖励。LIN：：还可以对完整迁移试算，选择收益更高的方案。考虑完整迁移比连续的拆分迁移收益更高的情况，jump也要考虑。
//                profitEntireRightNBSha = Math.abs(NB_MigrateLoad - Math.abs(NB_MigrateLoad - 2 * emitNBShardLoad)) + entireRewards;   ///TODO:理论上有赋值
//            }
//
//            ////jump迁移，计算在迁出节点迁移哪个分片的收益高， ###目前的计算方法还不能区分它们。
////            if(sortStoreTuple.size() == 0 || sortJoinTuple.size() == 0)continue;
//            if(sortStoreTuple.size() == 0)continue; ///TODO:0103修改
////            tmpJumpCursorJ = 0;
//            tmpJumpCursorS = 0;
//            tempMigLoad = 0l;
//            long joinedTupleStep = (long) (1.0/sortStoreTuple.size() * emigraShard._numTuplesJoined);///emigraShard.getRangeCountTuple(emigraShard._minRange, tmpKey);////TODO:0103膝盖
//            long immTupleJoinedNum = 0l;
//            while ((tempMigLoad < jumpMigrateLoad) && (sortStoreTuple.size() >= tmpJumpCursorS + 1)) {
//                Long joinKey = sortStoreTuple.get(tmpJumpCursorS);////TODO:这里最初的问题是数组越界，在条件判定加了一个判定后，虽然好了，但是这里逻辑不对~ 不应该存在tempMigLoad > migrateLoad,而且前面的东西都没打印出来。
///*                for (; tmpJumpCursorJ < sortJoinTuple.size(); tmpJumpCursorJ++) {
//                    if (sortJoinTuple.get(tmpJumpCursorJ) > joinKey) break;
//                }*/
//                immTupleJoinedNum = immTupleJoinedNum + joinedTupleStep;
////                long immTupleJoinedNum = emigraShard.getRangeCountTuple(emigraShard._minRange, joinKey);////TODO:0103修改
//                ////TODO(Lt):计算方法有问题，未考虑迁出task的负载下降部分
//                tempMigLoad = (long) immTupleJoinedNum * (tmpJumpCursorS+1) * _storeSampleStep;
//                tmpJumpCursorS += 1;
//            }
//            penaltyLoad = JumpPenalty(emigraShard, tmpJumpCursorS);
//            double profit = Math.abs(jumpMigrateLoad - Math.abs(jumpMigrateLoad - 2 * tempMigLoad)) - penaltyLoad * Math.pow((immJumpTaskShardNum),2) * _alpha1; ///jump migration，非shard整体迁移
//
//            if (maxJumpProfit < (long)profit) {
//                maxJumpProfit = (long) profit;
//                maxJumpEmiShardID = emigraShard._shardID;
//                if (tmpJumpCursorS > 0) {
//                    maxJumpCursorSKey = sortStoreTuple.get(tmpJumpCursorS - 1);
//                } else {
//                    maxJumpCursorSKey = emigraShard._minRange;///TODO:原来没有条件判定，为什么会导致下面的sortStoreTuple.get(jumpCursorS)数组越界？
//                }
//                ///修改迁出shard的minRange,(maxRange)
//                jumpEmiMinRange = emigraShard._minRange;
//                if (maxJumpCursorSKey < jumpEmiMinRange) {
////                     output("Error! XXXXX, maxJumpCursorSKey < jumpEmiMinRange!!!");
////                    maxJumpCursorSKey = jumpEmiMinRange;
////                    maxJumpProfit = -1l;
//                }
//            }
//        }
//
//        ////从5-6个收益中找最大的
//        double maxNBProfit = 0.0;
//        if(leftRightNB.equals("left")){
//            maxNBProfit = profitLeftNBLoad;
//            if(maxNBProfit < profitEntireLeftNBSha && _shardContainer.size() > 1){
//                nbCursorSKey = nbEmitMaxRange;
//                maxNBProfit = profitEntireLeftNBSha;
//            }
//        }else if(leftRightNB.equals("right")){
//            maxNBProfit = profitRightNBLoad;
//            if(maxNBProfit < profitEntireRightNBSha && _shardContainer.size() > 1){
//                nbCursorSKey = nbEmitMinRange;
//                maxNBProfit = profitEntireRightNBSha;
//            }
//        }
//
//        /*DEBUG_LOG*/{
//            output("-----------migrate(): Compute Profit-------------" + "\n");
//            output(" maxNBProfit " +", " + "maxJumpProfit" +", "+ "choose" + "\n");
//
//            String chooseString = ((long)maxNBProfit >= maxJumpProfit.longValue())? leftRightNB : "jump";
//            output( (long)maxNBProfit +", " + maxJumpProfit.intValue() +", " + chooseString + "\n");
//            output("Rewards of entire shard migration" + ",  penalty " + ", immJumpTaskShardNum" + "\n");
//            output( entireRewards + ",  " +  penaltyLoad * Math.pow((immJumpTaskShardNum),2) * _alpha1 + ",  " + immJumpTaskShardNum + "\n");
//            output("profitLeftNBLoad " + ", profitEntireLeftNBSha=" + ", profitRightNBLoad" + ",profitEntireRightNBSha= " + "\n");
//            output(profitLeftNBLoad + ", " + profitEntireLeftNBSha + "," + profitRightNBLoad + ", " + profitEntireRightNBSha + "\n");
//        }
//
//        //        if(maxJumpProfit > 0) maxJumpProfit = maxJumpProfit + 100000000;///在强制跳跃迁移
////        if(tempMigLoadNB.intValue() < maxJumpProfit.intValue() && immJumpTaskShardNum < _numShard) {
//        if(maxNBProfit < maxJumpProfit && immJumpTaskShardNum < _numShard) {   //TODO:
//            /// 修改迁出shard的minRange，通知迁出task新建shard，emit出所有元组。
//            modifyMigrateRegion("jump", maxJumpEmiShardID, immJumpTaskID, Long.MIN_VALUE, jumpEmiMinRange, maxJumpCursorSKey, Long.MIN_VALUE, collector);
//        }else{
//            ////说明是连续迁移，修改两个shard的minRange，maxRange。通知reshuffler修改路由，通知迁入task修改信息，emit出所有元组
//            modifyMigrateRegion(leftRightNB, emigrateNBShardID, immNBTaskID, immigrateNBShardID, nbEmitMinRange, nbCursorSKey, nbEmitMaxRange, collector);
//        }
//
//    }



    private long computeLoadBanlanceProfit(long deltaLoad,
       long emiStored, long emiJoined, long migStored, long migJoined, long immStored, long immJoined){
        //迁出减少的负载
        long dec = emiStored * emiJoined - (emiStored - migStored)*(emiJoined - migJoined);
        //迁入增加的负载
        long add = (immStored + migStored)*(immJoined + migJoined) - immStored * immJoined;
        //收益 = 旧负载差 - 新负载差
        long loadBanlanceProfit = deltaLoad - Math.abs(deltaLoad - dec - add);
        return loadBanlanceProfit;
    };


    private void migrate(Long minId , String minLoad, String nbLowShard, BasicOutputCollector collector){ //0104重写
        //采样结构
        List<Long> sortStoreTuple =  new ArrayList<>();
        Long taskLoadSelf = getTaskLoad();
        //解析迁移建议
        String []minTaskLoad = minLoad.split(",");
        String []nbLowSha = nbLowShard.split(",");

        Long immJumpTaskID = Long.parseLong(minTaskLoad[0]);
        Long immJumpTaskShardNum = Long.parseLong(minTaskLoad[1]);
        Long immJumpTaskLoad = Long.parseLong(minTaskLoad[2]);
        Long jumpMigrateLoad = taskLoadSelf - immJumpTaskLoad;

        Long immigrateNBShardID = Long.parseLong(nbLowSha[0]);
        Long immMinRangeNB = Long.parseLong(nbLowSha[1]);
        Long immMaxRangeNB = Long.parseLong(nbLowSha[2]);
        //nbLowSha[3]
        Long immNBTupleStoreNum = Long.parseLong(nbLowSha[4]);
        Long immNBTupleJoinNum = Long.parseLong(nbLowSha[5]);
        Long emTaskIdNB = Long.parseLong(nbLowSha[6]);
        Long emigrateNBShardID = Long.parseLong(nbLowSha[7]);
        String leftRightNB = String.valueOf(nbLowSha[8]);
        Long immNBTaskID = Long.parseLong(nbLowSha[9]);
        Long immNBTaskLoad = Long.parseLong(nbLowSha[10]);///numTmTupleStoreNB * nuupleJoinNB;///
        Long NB_MigrateLoad = taskLoadSelf - immNBTaskLoad;

        Long immNBShardLoad = immNBTupleStoreNum * immNBTupleJoinNum;

        //最大收益变量
        //jump migration
        long JUMP_profit = -1l;
        long JUMP_emigrateShardID = 0l;
        long JUMP_immTaskID = 0l;
        long JUMP_immigrateShardID = 0l;
        long JUMP_minRange = 0l;
        long JUMP_cursor = 0l;
        long JUMP_maxRange = 0l;
//        modifyMigrateRegion("jump", maxJumpEmiShardID, immJumpTaskID, Long.MIN_VALUE, jumpEmiMinRange, maxJumpCursorSKey, Long.MIN_VALUE, collector);
        //migrate to neighbor
        long NB_profit = -1l;
        long NB_emigrateShardID = 0;
        long NB_immTaskID = 0;
        long NB_immigrateShardID = 0;
        long NB_minRange = 0l;
        long NB_cursor = 0l;
        long NB_maxRange = 0l;
//        modifyMigrateRegion(leftRightNB, emigrateNBShardID, immNBTaskID, immigrateNBShardID, nbEmitMinRange, nbCursorSKey, nbEmitMaxRange, collector);

        /*DEBUG_LOG*/{
            output("-----------migrate(): Recv Suggestion-------------" + "\n");
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
            }
        }

        //计算奖励和惩罚
//        long entireRewards = jumpMigrateLoad / 4;///TODO:这两行，改。。。。
        long entireRewards = (1 - (double) tmpNBCursorS / (double) sortStoreTuple.size()) * emitNBShardLoad * _alpha2;
        long penaltyLoad = - NB_MigrateLoad / 16;

//            penaltyLoad = JumpPenalty(emigraShard, tmpJumpCursorS);
//            double profit = Math.abs(jumpMigrateLoad - Math.abs(jumpMigrateLoad - 2 * tempMigLoad)) - penaltyLoad * Math.pow((immJumpTaskShardNum),2) * _alpha1;
        for(int j = 0; j < _shardContainer.size(); j++) {
            //取得shard并采样
            Shard shardItr = _shardContainer.get(j);
            Long shardItrShardId = shardItr._shardID;
            sortStoreTuple = shardItr.SortStoreTuple();
            //有连续迁移建议的，计算连续迁移的两个收益
            if(emigrateNBShardID.longValue() == shardItrShardId.longValue()){
                NB_emigrateShardID = shardItr._shardID;
                NB_immTaskID = immNBTaskID;
                NB_immigrateShardID = immigrateNBShardID;
                NB_minRange = shardItr._minRange;
                NB_maxRange = shardItr._maxRange;
                //为兜底撤销方案准备参数
                if(leftRightNB.equals("left")){ NB_cursor = shardItr._minRange; }
                else{ NB_cursor = shardItr._maxRange; }
                //①计算完整迁移并合并换取奖励方案的收益
                NB_profit = entireRewards
                        + computeLoadBanlanceProfit(NB_MigrateLoad,
                        shardItr._numTuplesStored, shardItr._numTuplesJoined,
                        shardItr._numTuplesStored, shardItr._numTuplesJoined,
                        immNBTupleStoreNum, immNBTupleJoinNum);
                //如果是有效方案分位点设为完全迁移
                if(NB_profit >= 0l)
                if(leftRightNB.equals("left")){ NB_cursor = shardItr._maxRange; }
                else{ NB_cursor = shardItr._minRange; }

                if(NB_profit >= NB_MigrateLoad) ;//其他分位点完全均衡也无法更优
                else{
                    //②计算连续迁移最佳收益分位点
                    long numStoredMigrationTuple = 0l;
                    long numJoinedMigrationTuple = 0l;
                    if(leftRightNB.equals("left")){
                        int tmpIdx = 0;
                        while(tmpIdx < sortStoreTuple.size()){
                             //假设左迁移至该位置
                            numStoredMigrationTuple = tmpIdx * _storeSampleStep;
                            numJoinedMigrationTuple = tmpIdx * shardItr._numTuplesJoined / sortStoreTuple.size();
                            //比较收益
                            long tmp_NB_profit = computeLoadBanlanceProfit(NB_MigrateLoad,
                                    shardItr._numTuplesStored, shardItr._numTuplesJoined,
                                    numStoredMigrationTuple, numJoinedMigrationTuple,
                                    immNBTupleStoreNum, immNBTupleJoinNum);
                            if(tmp_NB_profit > NB_profit){
                                NB_profit = tmp_NB_profit;
                                NB_cursor = sortStoreTuple.get(tmpIdx);
                            }
                            //移动到重复key末尾
                             while(tmpIdx + 1 < sortStoreTuple.size()
                                     && sortStoreTuple.get(tmpIdx + 1)==sortStoreTuple.get(tmpIdx)) tmpIdx++;
                             tmpIdx++;
                        }
                    }
                    else{
                        int tmpIdx = sortStoreTuple.size() - 1;
                        while(tmpIdx >= 0){
                            //移动到重复key开头
                            while(tmpIdx - 1 >= 0
                                    && sortStoreTuple.get(tmpIdx - 1)==sortStoreTuple.get(tmpIdx)) tmpIdx--;
                            //假设右移至该位置
                            numStoredMigrationTuple = (sortStoreTuple.size() - tmpIdx + 1) * _storeSampleStep;
                            numJoinedMigrationTuple = (sortStoreTuple.size() - tmpIdx + 1) * shardItr._numTuplesJoined / sortStoreTuple.size();
                            //比较收益
                            long tmp_NB_profit = computeLoadBanlanceProfit(NB_MigrateLoad,
                                    shardItr._numTuplesStored, shardItr._numTuplesJoined,
                                    numStoredMigrationTuple, numJoinedMigrationTuple,
                                    immNBTupleStoreNum, immNBTupleJoinNum);
                            if(tmp_NB_profit > NB_profit){
                                NB_profit = tmp_NB_profit;
                                NB_cursor = sortStoreTuple.get(tmpIdx);
                            }
                            tmpIdx--;
                        }
                    }
                }
            }

            //计算跳跃迁移的两个收益
            JUMP_immTaskID = immJumpTaskID;
            JUMP_immigrateShardID = 0l;
            //①跳跃完整迁移避免惩罚方案的收益
            long tmp_JUMP_profit = computeLoadBanlanceProfit(jumpMigrateLoad,
                    shardItr._numTuplesStored, shardItr._numTuplesJoined,
                    shardItr._numTuplesStored, shardItr._numTuplesJoined,
                    0l, 0l);
            if(tmp_JUMP_profit > JUMP_profit){
                JUMP_profit = tmp_JUMP_profit;
                JUMP_emigrateShardID = shardItr._shardID;
                JUMP_minRange = shardItr._minRange;
                JUMP_cursor = shardItr._maxRange;
                JUMP_maxRange = shardItr._maxRange;
            }
            //②跳跃迁移最佳收益分位点
            long numStoredMigrationTuple = 0l;
            long numJoinedMigrationTuple = 0l;
            int tmpIdx = 0;
            while(tmpIdx < sortStoreTuple.size()){
                //假设左迁移至该位置
                numStoredMigrationTuple = tmpIdx * _storeSampleStep;
                numJoinedMigrationTuple = tmpIdx * shardItr._numTuplesJoined / sortStoreTuple.size();
                //比较收益
                tmp_JUMP_profit = penaltyLoad
                        + computeLoadBanlanceProfit(jumpMigrateLoad,
                        shardItr._numTuplesStored, shardItr._numTuplesJoined,
                        numStoredMigrationTuple, numJoinedMigrationTuple,
                        0, 0);
                if(tmp_JUMP_profit > JUMP_profit){
                    JUMP_profit = tmp_JUMP_profit;
                    JUMP_emigrateShardID = shardItr._shardID;
                    JUMP_minRange = shardItr._minRange;
                    JUMP_cursor = sortStoreTuple.get(tmpIdx);
                    JUMP_maxRange = shardItr._maxRange;
                }
                //移动到重复key末尾
                while(tmpIdx + 1 < sortStoreTuple.size()
                        && sortStoreTuple.get(tmpIdx + 1)==sortStoreTuple.get(tmpIdx)) tmpIdx++;
                tmpIdx++;
            }
        }
        //方案选择
        boolean validMigrateToNeighbor = NB_profit >= 0l;
        boolean validMigrateJump = JUMP_profit > 0l &&  immJumpTaskShardNum < _numShard;
        if(validMigrateToNeighbor && validMigrateJump) { //选择最佳方案
            if(JUMP_profit > NB_profit)
                modifyMigrateRegion("jump", JUMP_emigrateShardID, JUMP_immTaskID, JUMP_immigrateShardID, JUMP_minRange, JUMP_cursor, JUMP_maxRange, collector);
            else
                modifyMigrateRegion(leftRightNB, NB_emigrateShardID, NB_immTaskID, NB_immigrateShardID, NB_minRange, NB_cursor, NB_maxRange, collector);
        }
        else if(validMigrateToNeighbor && !validMigrateJump) //连续方案无效，只有跳跃迁移方案可选
            modifyMigrateRegion("jump", JUMP_emigrateShardID, JUMP_immTaskID, JUMP_immigrateShardID, JUMP_minRange, JUMP_cursor, JUMP_maxRange, collector);
        else if(!validMigrateToNeighbor && validMigrateJump) //跳跃方案无效，只有连续方案可选
            modifyMigrateRegion(leftRightNB, NB_emigrateShardID, NB_immTaskID, NB_immigrateShardID, NB_minRange, NB_cursor, NB_maxRange, collector);
        else //无方案可选，进行无影响连续迁移撤销
            modifyMigrateRegion(leftRightNB, NB_emigrateShardID, NB_immTaskID, NB_immigrateShardID, NB_minRange, NB_cursor, NB_maxRange, collector);

        /*DEBUG_LOG*/{
            output("-----------migrate(): Compute Profit-------------" + "\n");
            output(" NB_profit " +", " + "JUMP_profit" +", "+ "choose" + "\n");

            String chooseString = "null";
            if(validMigrateToNeighbor && validMigrateJump) //选择最佳方案
                if(JUMP_profit >= NB_profit) chooseString = "jump";
                else chooseString = leftRightNB;
            else if(validMigrateToNeighbor && !validMigrateJump) //连续方案无效，只有跳跃迁移方案可选
                chooseString = "jump";
            else if(!validMigrateToNeighbor && validMigrateJump) //跳跃方案无效，只有连续方案可选
                chooseString = leftRightNB;
            else //无方案可选，进行无影响迁移撤销
                chooseString = "recall";

            output( NB_profit +", " + JUMP_profit +", " + chooseString + "\n");
            output("entireRewards" + ",  penalty " + ", immJumpTaskShardNum" + "\n");
            output( entireRewards + ",  " +  penaltyLoad + ",  " + immJumpTaskShardNum + "\n");
        }

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
                    collector.emit(JOINER_TO_RESHUFFLER_STREAM_ID, new Values("modify", _taskRel, emigrateShardID, immTaskID, immigrateShardID, Long.MIN_VALUE, cursorL, Long.MIN_VALUE, numMigrateTuple));//minLoadId还可能是迁入的哪个邻居的id。

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
                    collector.emit(JOINER_TO_RESHUFFLER_STREAM_ID, new Values("modify", _taskRel, emigrateShardID, immTaskID, immigrateShardID, Long.MAX_VALUE, cursorL, Long.MAX_VALUE, numMigrateTuple));//minLoadId还可能是迁入的哪个邻居的id。

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
                    if(minRange < cursorL){
                        collector.emitDirect(immTaskID.intValue(), JOINER_TO_JOINER_MODIFY_STREAM_ID, new Values("shard", jumpImmShardID, leftRightOrJump, minRange, cursorL, immTaskID));
                        numMigrateTuple =  _shardContainer.get(m).immigrateTuples(immTaskID.intValue(), jumpImmShardID, "left", cursorL, collector);
                        numMigrateJoinedTuple = _shardContainer.get(m).subJoinedTupleNum(immTaskID.intValue(), jumpImmShardID, numMigrateTuple, collector);
                    }
                    collector.emit(JOINER_TO_RESHUFFLER_STREAM_ID, new Values("shard", _taskRel, emigrateShardID, immTaskID, jumpImmShardID, minRange, cursorL, maxRange, numMigrateTuple));//minLoadId还可能是迁入的哪个邻居的id。
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
                    /*DEBUG_LOG*/
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

    private double JumpPenalty(Shard emitShard, int tmpJumpCursorS){ ////SelfLoad * （e1 + e2)/(minRagen + cursor)
        double penaltyLoad = 0l;
        if(emitShard._minRange + tmpJumpCursorS != 0)
        penaltyLoad = ((_e1 + _e2)/(emitShard._minRange + tmpJumpCursorS)) * emitShard._numTuplesStored * emitShard._numTuplesJoined;
        return penaltyLoad;
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

}
