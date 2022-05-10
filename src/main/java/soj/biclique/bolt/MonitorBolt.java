package soj.biclique.bolt;

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import static com.google.common.collect.Lists.newArrayList;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.shade.org.apache.commons.collections.map.HashedMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import soj.util.FileWriter;
import soj.util.Stopwatch;

import static com.google.common.collect.Lists.newLinkedList;
import static java.util.concurrent.TimeUnit.*;
import static soj.biclique.KafkaTopology.MONITOR_R_TO_JOINER_STREAM_ID;
import static soj.biclique.KafkaTopology.MONITOR_S_TO_JOINER_STREAM_ID;
import static soj.util.CastUtils.*;
import soj.biclique.Shard.Shard;
import soj.biclique.router.MonitorTable;

public class MonitorBolt extends BaseRichBolt{
    private static final List<String> TO_JOINER_MIGRATE_SCHEMA = ImmutableList.of("type", "minLoadId", "minLoad", "nbLowShard", "B1AddSh", "B1ReSh", "n0");
    private int _tid;
    private FileWriter _output, _outputResh;
    private String _rel;
    private int _boltsNum;
    private Map<Integer, Long> _relCounter;
    private Map<Integer, Long> _oppRelCounter;
    private Map<Integer, FileWriter> _loadOutput;
    private long _lastOutputTime;
    private Stopwatch _stopwatch;
    private long _startTime;
    private int _startMonitorTime;
    private double _threshold;
    private double _interval;
    OutputCollector _collector;
    private Queue<Pair> _countStoreQ, _countJoinQ;
    private long _tsQueue,_nextTime;
    private int _counterStore, _counterJoin;
    private long _LoadPerbolt[][];
    private Map<Integer, Long> _numPerInter, _numOppPerInter;
    private Map<Integer, Values> _statistics, _oppStatist;
    private Queue<Pair> _staQueue, _staOppQue;
    private int _reshufflesNum;
    private boolean _isWindow;
    private double _winSize;
    private Map<Integer, Tuple>  _statisBolt;
    private Map<Long, List<Shard>> _joinerShards;
    private MonitorTable _monitorTable;
    private long _maxTaskId;
    private Map<Integer, Long> _taskBarrier;
    private Map<Integer, Long> _barrierTimer;
    private boolean _finishMigrate;
    private boolean _initialJoiner, _migOrNot;
    private boolean _buildOutput;
    private int _numInitShard;
    private Map<Integer, Values> _staTestReshuffle;
    private int _totalNumRealEmit;
    private long _totalES = 0L, _totalNumReceiT;
    private boolean _initialES;
    private double _ES1, _lamda, _miu1, _PQ, _rou1, ES1H, _miu1A, _PQA, _rou1A, _B1, _B1_AddSha, _B1_ReduceSha, _ET1, _ET1_AddSha, _ET1_ReduceSha;
    private long _lastSecond, _e1, _e2;
    private double _n0;

    public MonitorBolt(String rel, int boltsNum, boolean win, double winsize, int startTime, double threshold, boolean migOrNot,
                       double interval, int reshuffleNum, int numInitShardPerTask, long e1, long e2) {
        _rel = rel;
        _boltsNum = boltsNum;
        _relCounter = new HashedMap();
        _oppRelCounter = new HashedMap();
        _loadOutput = new HashedMap();
        _statistics = new HashMap<>();
        _oppStatist = new HashMap<>();
        _startMonitorTime = startTime;
        _threshold = threshold;
        _interval = interval;
        _countStoreQ = newLinkedList();
        _countJoinQ = newLinkedList();
        _numPerInter = new HashedMap(_boltsNum*2);////存储每个时间段从reshuffle发过来的元组数量
        _numOppPerInter = new HashedMap(_boltsNum*2);/////这个monitor也应该知道另一条流的bolt的数量，目前让他们一样多吧
        _isWindow = win;
        _winSize = winsize;
        _reshufflesNum = reshuffleNum;
        _migOrNot = migOrNot;
        _numInitShard = numInitShardPerTask;
        _e1 = e1;
        _e2 = e2;
        _totalNumRealEmit = 0;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _tid = context.getThisTaskId();
        String prefix = "ysy_monitor_" + _rel.toLowerCase() + _tid;
//        String prefix2 = "ysy_monitorResh_" + _rel.toLowerCase() + _tid;
        _output = new FileWriter("/home/shuiyinyu/join/tmpresult-fastjoin", prefix, "csv").setFlushSize(10);//.setFlushSize(10).setPrintStream(System.out);
//        _outputResh = new FileWriter("/home/shuiyinyu/join/tmpresult-fastjoin", prefix2, "csv");
        _stopwatch = Stopwatch.createStarted();
        _lastOutputTime = _stopwatch.elapsed(MILLISECONDS)+3*(long)_interval;
        _collector = collector;
        _startTime = _stopwatch.elapsed(MILLISECONDS);
        _counterStore = 0;
        _counterJoin = 0;
        _LoadPerbolt = new long[_boltsNum][2];
        _nextTime = _stopwatch.elapsed(MILLISECONDS);
        _staQueue = newLinkedList();
        _staOppQue = newLinkedList();
        _tsQueue = _stopwatch.elapsed(MILLISECONDS);
        _statisBolt = new HashMap<Integer, Tuple>();
        _joinerShards = new HashMap<Long, List<Shard>>();
        _monitorTable = new MonitorTable(_boltsNum);
        _maxTaskId = 0l;
        _taskBarrier = new HashMap<Integer, Long>();
        _barrierTimer = new HashMap<Integer, Long>();
        _finishMigrate = true;
        _initialJoiner = false;
        _buildOutput = false;
        _staTestReshuffle =  new HashMap<Integer, Values>();
        _initialES = false;
        _ET1 = _ES1 = _lamda = _PQ = _rou1 = ES1H = _PQA = _rou1A = _B1 = _ET1_AddSha = _ET1_ReduceSha = _B1_AddSha = _B1_ReduceSha = 0.0;
        _lastSecond = 0L;
        _totalNumReceiT = 0L;
        _n0 = 0.0;
    }

    @Override
    public void execute(Tuple input) {
//        output("The first field:" + (Object)input.getValue(0));
//        output("The second field:" + (Object)input.getValue(1));
//        output("The 3rd field:" + (Object)input.getValue(2)); //ImmutableList.of("type", "relation", "timestamp", "statistics")
//        Values v = new Values(input.getStringByField("relation"),
//                input.getLongByField("timestamp"), input.getStringByField("statistics"));
        String type = input.getStringByField("type");

        long curr = _stopwatch.elapsed(MILLISECONDS);

        //record signal
        if (type.equals("test")){
            int reshuffleId = input.getSourceTask();
/*            if(!_staTestReshuffle.containsKey(reshuffleId)){
                _staTestReshuffle.put(reshuffleId, );
            }*/
            int numRealEmit = input.getIntegerByField("emitTaskID");////借用的别的SCHEMA。。。
            long numReceiveT = input.getLongByField("immTaskID");////借用的别的SCHEMA。。。
            long ES = input.getLongByField("emiShardID");
            _totalNumRealEmit += numRealEmit;
            _totalES += ES;
            _totalNumReceiT += numReceiveT;
            if(curr - _lastSecond > 1000){
                /*DEBUG_LOG*/
                _n0 = (double)_totalNumRealEmit/_totalNumReceiT;
                _lamda = _totalNumReceiT;
                _totalNumRealEmit = 0;
                _totalES = 0L;
                _lastSecond = curr;
                _totalNumReceiT= 0L;
            }
        }
        else if(type.equals("recordSignal-bolt")){
            int taskboltId = input.getSourceTask();
//            _monitorTable.creatTask(taskboltId);
            String statistics = input.getStringByField("statistics");
            /*DEBUG_LOG*/
            if(_buildOutput){
                output("  [ Load Report ]  " + " @tsk: " + taskboltId + "\n");
            }

            ////解析String为多个shard
            String []statistic = statistics.split(";");
            if(statistic[0].length()!=0){
                if(_buildOutput){
//                    output("statistics=" + statistics + ",statistics.length()=" + statistics.length() + ",statistic= " + statistic.length + "\n");
                }
                for(int i = 0; i < statistic.length-1; i++){
                    Shard shard = new Shard(statistic[i], taskboltId);
                    _monitorTable.setOrCreatShard(shard);
                /*DEBUG_LOG*/
                    if(_buildOutput){
                    output("-item- " + statistic[i] + "\n");
                    }
                }
            }
            double miu = Double.parseDouble(statistic[statistic.length-1]);
            _monitorTable.setTaskMiu(taskboltId, miu);

            /*DEBUG_LOG*/
            if(_buildOutput){
                output("  [-]  " + "\n");
            }

            if(_monitorTable._shardNum >= _boltsNum * _numInitShard)_initialJoiner = true;

            if(curr < (_winSize * 2)) {
                _initialJoiner = false;
            }

            /////一段时间_interval之后统计一次负载，得到最大负载和最小负载的joinerID。
            if((curr - _lastOutputTime >= _interval) && _finishMigrate && _initialJoiner){
//                ////TODO:用来观察负载变化，测试完删除
//               output(_stopwatch.elapsed(SECONDS) + "," + _monitorTable.getAllTaskLoadString().toString() + "\n");
//               output("the diameter and average distance:" + "," + _monitorTable.getNetworkDiameter() + "\n");

                if(_totalES > 0){
                    _ES1 = (double)_totalES/_totalNumReceiT;
                    _miu1 = (1/_ES1)*1000000;
                    _ET1 = computeET(_miu1);
                    double n0_AddSha = _n0 + (double) (_e1 + _e2)/8200;
                    double miu_AddShard = (_n0/n0_AddSha) * _miu1;
                    _ET1_AddSha = computeET(miu_AddShard);
                    _B1_AddSha = _ET1 - _ET1_AddSha;
                    double n0_ReduceSha = _n0 - (double) (_e1 + _e2)/8200;
                    double miu_ReduceShard = (_n0/n0_ReduceSha) * _miu1;
                    _ET1_ReduceSha = computeET(miu_ReduceShard);
                    _B1_ReduceSha = _ET1 - _ET1_ReduceSha;
//                    output("_ET1=" + _ET1 + ",_ET1_AddSha=" + _ET1_AddSha + ",miu_AddShard=" + miu_AddShard + ",miu_ReduceShard=" + miu_ReduceShard + "\n");
                }
//                if(_n0>20) _ET1 = 100000 * _n0;
//                _ET1 = 4 + 5*_n0;////通过routers的数量为6时，输入流速47000，不断改变n0的值，计算得出这个关系。当_n0>20，ET不符合这个关系，增大
                List<Long> maxMintask = _monitorTable.getMaxMinTask();
                _maxTaskId = maxMintask.get(0);
                long maxLoad = maxMintask.get(1);
                long minTaskId = maxMintask.get(2);
                long minLoad = maxMintask.get(3);
                double LI = 0.0;
                if(maxLoad > 0 && minLoad > 0 && maxLoad >= minLoad)
                   LI = (double) maxLoad / (double) minLoad;
                else if(maxLoad > 0 && minLoad == 0 && maxLoad >= minLoad)
                   LI = (double) maxLoad;
                if(_buildOutput){
                   /*DEBUG_LOG*/
                    output("  [ Unbalance Computation ] @sec " + _stopwatch.elapsed(SECONDS) + " LI," + LI + ",num shards=," + _monitorTable._shardNum + "\n");
                    output(" LI" +"," + "_maxTaskId" + "," + "maxLoad" + "," + "minTaskId" + "," + "minLoad" + "\n");
                }
                output( LI +"," + _maxTaskId +","+ maxLoad +"," + minTaskId +","+ minLoad+ "\n");

               if(LI > _threshold){
                   String streamId = MONITOR_R_TO_JOINER_STREAM_ID;
                   if(_rel.equals("S")){
                       streamId = MONITOR_S_TO_JOINER_STREAM_ID;
                   }

                   String debug_getTaskToString = _monitorTable.getTaskToString(minTaskId);
                   String debug_getLowLoadNeighboursS = _monitorTable.getLowLoadNeighboursS(_maxTaskId);
                   /*DEBUG_LOG*/
                   if(_buildOutput){
                       /*DEBUG_LOG*/
                       output(_monitorTable.outputLoadMiu());
                       output("  [ Start Migration ]  " + " @tsk: " + _maxTaskId + "startTime" + "\n");
                       output("To NB:" + System.currentTimeMillis() + "\n");
                       output(debug_getLowLoadNeighboursS + "\n");
                       output("To MIN:  " + " @tsk: " + minTaskId + "\n");
                       output(debug_getTaskToString + "\n");
                       output("  [-]  " + "\n");
                   }

                   if(_migOrNot){
                       _finishMigrate = false;
//                       output(_monitorTable.getTaskToString(minTaskId) + "," + _monitorTable.getLowLoadNeighboursS(_maxTaskId) + "\n");
                       _collector.emitDirect((int)_maxTaskId, streamId, new Values("migration", minTaskId, _monitorTable.getTaskToString(minTaskId),
                               _monitorTable.getLowLoadNeighboursS(_maxTaskId), _B1_AddSha, _B1_ReduceSha, _n0)); /////不光要发送相邻的shard，还要发送相邻shard的task的总负载。
                       ///从reshuffle发送过来的修改monitorTable的消息对齐
                       if(!_barrierTimer.containsKey((int)_maxTaskId)) {
                           _barrierTimer.put((int)_maxTaskId, _stopwatch.elapsed(MILLISECONDS));
                       }
                   }
               }
               else{
//                   output("--------------------");
//                   output(_stopwatch.elapsed(SECONDS) + "," + _monitorTable.getAllTaskLoadString().toString() + "\n");
//                   output("the diameter and average distance:" + "," + _monitorTable.getNetworkDiameter() + "\n");
               }
               /////shard左右相邻的task的id，存储和连接元组的数量，总负载。
               _lastOutputTime = curr;
//                }
            }
        }else if(type.equals("modify")){  ////reshuffle通知monitor同步shard修改的信息  type, rel, emiShardID, immTaskID, immShardID, minRange, cursor, maxRange
//            Long rel = input.getLongByField("relation");  "type", "relation", "emitTaskID", "emiShardID", "immTaskID", "immShardID", "minRange", "cursor", "maxRange"
            int emiTaskID = input.getIntegerByField("emitTaskID");
            Long emiShardID = input.getLongByField("emiShardID");
            int immTaskID = input.getIntegerByField("immTaskID");
            Long immShardID = input.getLongByField("immShardID");
            Long minRange = input.getLongByField("minRange");
            Long cursor = input.getLongByField("cursor");
            Long maxRange = input.getLongByField("maxRange");
            Long numMigrateTuple = input.getLongByField("numMigrateTuple");

            /*DEBUG_LOG*/
            if(_buildOutput){
                output("  [ Modify Quantile ]  " + "\n");
                output(" @tsk-SD: " + emiTaskID + "-"  +emiShardID + " to @tsk-SD: " + immTaskID + "-"  +immShardID+ "\n");
            }

            if(BarrierFromReshuffle(emiTaskID)){
                _finishMigrate = true;
                long migrationUseTime = _stopwatch.elapsed(MILLISECONDS) - _barrierTimer.remove(emiTaskID);
                /*DEBUG_LOG*/
                if(_buildOutput)
                {
                    output("  [ Modify Barrier ]  " + " @tsk: " + emiTaskID
                        + " migrationUseTime = " + migrationUseTime + "ms" + "\n");
                    output("BarrierFromReshuffle() is true, the time:" + System.currentTimeMillis() + "\n");
                }
            }else{
//                output("BarrierFromReshuffle() is false, the time:" + System.currentTimeMillis() + "\n");
            }

            if(minRange == Long.MIN_VALUE && _finishMigrate){ ///向左迁移
                Shard emitShard = new Shard(emiShardID, cursor, Long.MIN_VALUE, emiTaskID, numMigrateTuple, 0l);
                boolean setLeft = _monitorTable.setLeftQuantile(emitShard, cursor);
                Shard immShard = new Shard(immShardID, Long.MIN_VALUE, cursor, immTaskID, numMigrateTuple, 0l);
                boolean setRight = _monitorTable.setRightQuantile(immShard, cursor);///.setOrCreatShard(emitShard)

                /*DEBUG_LOG*/
                if(_buildOutput){
                    output(" _rel " + _rel + "\n");
                    output(" Tsk" +", " + "SD" + ", " + "<<<<<<<<<" + ", " + "Tsk" + ", " + "SD" + "\n");
                    output( immTaskID +", " + immShardID +", "+ "<<<<<<<<<" +", " + emiTaskID +", "+ emiShardID+ "\n");
                    output(" minRange" +", " + "cursor" + ", " + "maxRange" + ", " + "numMigrateTuple" + "\n");
                    output( minRange +", " + cursor +", "+ maxRange +", " + numMigrateTuple + "\n");
                    output("-emitShard- " + "\n");
                    output(emitShard.encodeShardInJoiner() + "\n");
                    output("-immShard- " + "\n");
                    output( immShard.encodeShardInJoiner() + "\n");
                    output("_monitorTable.setLeftQuantile()"+ "  " + setLeft + "\n");
                    output("(emitShard, cursor)" + "\n");
                    output("emitShard" +", " + cursor + "\n");
                    output("_monitorTable.setRightQuantile()" + "  " + setRight + "\n");
                    output("(immShard, cursor)" + "\n");
                    output("immShard" +", " + cursor + "\n");
                }

            }else if(minRange == Long.MAX_VALUE && _finishMigrate){ ///向右迁移
                Shard emitShard = new Shard(emiShardID, Long.MIN_VALUE, cursor, emiTaskID, numMigrateTuple, 0l);
                boolean setRight = _monitorTable.setRightQuantile(emitShard, cursor);
                Shard immShard = new Shard(immShardID, cursor, Long.MIN_VALUE, immTaskID, numMigrateTuple, 0l);
                boolean setLeft = _monitorTable.setLeftQuantile(immShard, cursor);

                /*DEBUG_LOG*/
                if(_buildOutput){
                    output(" _rel " + _rel + "\n");
                    output(" Tsk" +", " + "SD" + ", " + ">>>>>>>>>" + ", " + "Tsk" + ", " + "SD" + "\n");
                    output( emiTaskID +", " + emiShardID +", "+ ">>>>>>>>>" +", " + immTaskID +", "+ immShardID+ "\n");
                    output(" minRange" +", " + "cursor" + ", " + "maxRange" + ", " + "numMigrateTuple" + "\n");
                    output( minRange +", " + cursor +", "+ maxRange +", " + numMigrateTuple + "\n");
                    output("-emitShard- " + "\n");
                    output(emitShard.encodeShardInJoiner() + "\n");
                    output("-immShard- " + "\n");
                    output( immShard.encodeShardInJoiner() + "\n");
                    output("_monitorTable.setLeftQuantile()"+ "  " + setLeft + "\n");
                    output("(emitShard, cursor)" + "\n");
                    output("emitShard" +", " + cursor + "\n");
                    output("_monitorTable.setRightQuantile()" + "  " + setRight + "\n");
                    output("(immShard, cursor)" + "\n");
                    output("immShard" +", " + cursor + "\n");
                }
            }

            /*DEBUG_LOG*/
            if(_buildOutput){
                String debugMonitor = _monitorTable.outPutAllMonitorTableItems("After [ Modify Quantile ] Print MonitorTable" );
                output(debugMonitor + "\n");
                output("  [-]  " + "\n");
            }

        }
        else if(type.equals("shard")){  ////reshuffle通知monitor同步shard新建的信息；记得检查当joiner发送过来新建的shard的时候，monitorTable的处理方式。
//            Long rel = input.getLongByField("relation");
            int emiTaskID = input.getIntegerByField("emitTaskID");
            Long emiShardID = input.getLongByField("emiShardID");
            int immTaskID = input.getIntegerByField("immTaskID");
            Long immShardID = input.getLongByField("immShardID");
            Long minRange = input.getLongByField("minRange");
            Long cursor = input.getLongByField("cursor");
//            Long maxRange = input.getLongByField("maxRange");
            Long numMigrateTuple = input.getLongByField("numMigrateTuple");

            /*DEBUG_LOG*/
            if(_buildOutput){
                output("  [ New Shard ]  " + "\n");
                output(" @tsk-SD: " + emiTaskID + "-"  +emiShardID + " to @tsk-SD: " + immTaskID + "-"  +immShardID+ "\n");
            }

            if(BarrierFromReshuffle(emiTaskID)){
                _finishMigrate = true;
                long migrationUseTime = _stopwatch.elapsed(MILLISECONDS) - _barrierTimer.remove(emiTaskID);
                /*DEBUG_LOG*/
                if(_buildOutput){
                output("  [ New shard Barrier ]  " + " @tsk: " + emiTaskID
                        + " migrationUseTime = " + migrationUseTime + "ms" + "\n");
                    output("BarrierFromReshuffle() is true, the time:" + System.currentTimeMillis() + "\n");
                }
            }else{
//                output("BarrierFromReshuffle() is false, the time:" + System.currentTimeMillis() + "\n");
            }

            if(minRange.longValue() < cursor.longValue() && _finishMigrate){
                Shard emitShard = new Shard(emiShardID, cursor, Long.MIN_VALUE, emiTaskID, numMigrateTuple, 0l);
                boolean debug_setLeftQuantile = _monitorTable.setLeftQuantile(emitShard, cursor);
                Shard immShard = new Shard(immShardID, minRange, cursor, immTaskID, numMigrateTuple, 0l);
                boolean debug_addShardToRangeList = _monitorTable.addShardToRangeList(immShard);
                /*DEBUG_LOG*/
                if(_buildOutput){
                    output(" _rel " + _rel + "\n");
                    output(" Tsk" +", " + "SD" + ", " + " <<<~~~<<<" + ", " + "Tsk" + ", " + "SD" + "\n");
                    output( immTaskID +", " + immShardID +", "+ "<<<~~~<<<" +", " + emiTaskID +", "+ emiShardID+ "\n");
                    output(" minRange" +", " + "cursor" + ", " + "numMigrateTuple" + "\n");
                    output( minRange +", "  + cursor +", "  + numMigrateTuple + "\n");
                    output("-emitShard- " + "\n");
                    output(emitShard.encodeShardInJoiner() + "\n");
                    output("-immShard- " + "\n");
                    output( immShard.encodeShardInJoiner() + "\n");
                    output("_monitorTable.setLeftQuantile()"+ "  " + debug_setLeftQuantile + "\n");
                    output("(emitShard, cursor)" + "\n");
                    output("emitShard" +", " + cursor + "\n");
                    output("_monitorTable.addShardToRangeList()" + "  " + debug_addShardToRangeList + "\n");
                    output("(immShard)" + "\n");
                    output("immShard" + "\n");

//                    _monitorTable.outPutAllMonitorTableItems("After [ New Shard ] Print MonitorTable" );
                    output("  [-]  " + "\n");
                }
            }
        }
        else if(type.equals("initialShard")){  ////"type", "relation", "emitTaskID", "emiShardID", "immTaskID", "immShardID", "minRange", "cursor", "maxRange"
            Long immShardID = input.getLongByField("immShardID");
            Long minRange = input.getLongByField("minRange");
            Long cursor = input.getLongByField("cursor");
            int immTaskID = input.getIntegerByField("immTaskID");
//            Long numMigrateTuple = input.getLongByField("numMigrateTuple");
            Shard immShard = new Shard(immShardID, minRange, cursor, immTaskID, 0l, 0l);

            boolean debug_addShardToRangeList = _monitorTable.addShardToRangeList(immShard);

            /*DEBUG_LOG*/
            if(_buildOutput){
                output("  [ Initial Shard ]  " + "\n");
                output(" @tsk-SD: " + immTaskID + "-" + immShardID + "\n");
                output("Range: [ " + minRange + ", " + cursor + ")" + "\n");
                output("-immShard- " + "\n");
                output( immShard.encodeShardInJoiner() + "\n");
                output("_monitorTable.addShardToRangeList()"+ "  " + debug_addShardToRangeList + "\n");
                output("(immShard)" + "\n");
                output("immShard" + "\n");

//                _monitorTable.outPutAllMonitorTableItems("After [ Initial Shard ] Print MonitorTable" );
                output("  [-]  " + "\n");
            }
        }

        //migration end signal
        else if(type.equals("migrationEndSignal")){
            Integer sid = input.getIntegerByField("sourceId");
            Integer tid = input.getIntegerByField("targetId");
            Long num = input.getLongByField("num");
            _relCounter.put(sid, _relCounter.get(sid) - num);
            _relCounter.put(tid, _relCounter.get(sid) + num);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(MONITOR_R_TO_JOINER_STREAM_ID, true, new Fields(TO_JOINER_MIGRATE_SCHEMA));
        declarer.declareStream(MONITOR_S_TO_JOINER_STREAM_ID, true, new Fields(TO_JOINER_MIGRATE_SCHEMA));
    }

    private boolean BarrierFromReshuffle(int emiTaskID){
        if(!_taskBarrier.containsKey(emiTaskID)){
            _taskBarrier.put(emiTaskID, 1l);///可以放个时间戳，用来容错，如果时间常不能barrier通过，要检查。
        }
        else _taskBarrier.put(emiTaskID, _taskBarrier.get(emiTaskID)+1);
        if(_taskBarrier.get(emiTaskID) >= _reshufflesNum){
            _taskBarrier.remove(emiTaskID);
            return true;
        }
        return false;
    }

    private void output(String msg) {
        if (_output != null){
            //_output.write(msg);
            _output.writeImmediately(msg);
        }
    }

    private void outputResh(String msg) {
        if (_outputResh != null){
            //_output.write(msg);
            _outputResh.writeImmediately(msg);
        }
    }

    private boolean isInWindow(long tsIncoming, long tsStored) {
        if(Math.abs(tsIncoming - tsStored) <= _winSize)return true;
        else {
            return false;
        }
    }
    private boolean isInJoinWindow(long tsIncoming, long tsStored) {
        if(Math.abs(tsIncoming - tsStored) <= 1000)return true;
        else {
            return false;
        }
    }

    private long factorial(long number) {
        if (number <= 1)
            return 1;
        else
            return number * factorial(number - 1);
    }

    private double computeET(double miu){
        double ET = 0.0;
        double rou1 = _lamda/(miu);
        double pai00 = 0.0;
        for(int i = 0; i < _reshufflesNum; i++){
            pai00 += (Math.pow(_reshufflesNum*rou1,i)/factorial(i));
        }
        double pai0 = 1.0/(pai00 + Math.pow(_reshufflesNum*rou1,_reshufflesNum)/(factorial(_reshufflesNum)*(1-rou1)));
        double PQ = Math.pow(_reshufflesNum*rou1, _reshufflesNum)*pai0/(factorial(_reshufflesNum)*(1-rou1));
        ET = (1/_lamda) * PQ * (rou1/(1-rou1)) + 1/miu;
        return ET;
    }

}
