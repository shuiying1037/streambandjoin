package soj.biclique.Shard;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
//import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.tuple.Values;
import soj.util.FileWriter;

import java.util.*;

import static com.google.common.collect.Lists.newLinkedList;
import static soj.biclique.KafkaTopology.*;
import static soj.util.CastUtils.*;
import static soj.biclique.KafkaTopology.JOINER_TO_JOINER_STREAM_ID;

public final class Shard{
    public Shard left;
    public Shard right;
    public long _shardID;
    public long _minRange;
    public long _maxRange;
    public long _owner;
    private int _state;
    public long _load;
    private String _statistics;
//    private Queue<String> immigrateShardID;
//    private Map<long,long> Tuples;

    private Queue<Pair> _indexQueue;
    private ArrayList<Values> _currList;
    private int _subindexSize;
    public boolean _window;
    private long _thisWinSize;

    public long _numTuplesStored, _numTuplesJoined, _numTuplesBeforeJ;////, _numSampleJoinTuples
//    private ArrayList<Long>  _oppRelKeyCurrList;
    private long _resultNum;
    private FileWriter _output;
    Deque<Long> deque = new LinkedList<Long>();
    private long _oppWinSize = _thisWinSize/4;
    private long _numCurrTupleJoined;
    private Queue<Pair> _oppRelCountQueue;
//    private final int _sampleStep = 10;
//    private static final List<String> SCHEMA = ImmutableList.of("type", "relation", "timestamp", "key", "value", "boltID", "shardID");//
    private final int _joinSampleStep = 30, _storeSampleStep = 30;//分别为连接元组和存储元组在joiner的采样排序的抽样比。其中_joinSampleStep为存储的时候就是抽样存储的，所以不用在此二次抽样。

//    private Queue<Pair> _oppImmRelNumQue;

    public Shard(long shardId, long minRange, long maxRange, long ownerID){ ////for reshuffleBolt
        left = null;
        right = null;
        this._shardID = shardId;
        this._minRange = minRange;
        this._maxRange = maxRange;
        this._owner = ownerID;
        this._state = 0; ///怎么样变成字符串表示？
/*        this._window = win;
        this._thisWinSize = winsize;
        this._subindexSize = subindexSize;*/
        this._statistics = "";
        _numTuplesStored = 0l;
        _numTuplesJoined = 0l;
        _numTuplesBeforeJ = 0;
        _load = _numTuplesStored * _numTuplesJoined;
///reshuffle 不需要存储，只需要穿参数
/*        _indexQueue = new LinkedList();
        _currList = new ArrayList<Values>();
        _oppRelKeyCurrList = new ArrayList<Long>();
        _oppRelQueue = new LinkedList(); ///长度为_oppRelQueLength*/

//        _numSampleJoinTuples = 0;
//        _joinSampleStep = 30;////在reshuffler可能用不到；
//        _storeSampleStep = 30;////在reshuffler可能用不到；
    }

    public Shard(long shardID, long minRange, long maxRange, long ownerID, int subindexSize, boolean win, long winsize){ //// 迁移后，迁入task的new Shard。
        left = null;
        right = null;
        this._shardID = shardID;
        this._minRange = minRange;
        this._maxRange = maxRange;
        this._owner = ownerID;
        this._state = 0; ///怎么样变成字符串表示？
        this._window = win;
        this._thisWinSize = winsize;
        this._subindexSize = subindexSize;
        this._statistics = "";
        _numTuplesStored = 0l;
        _numTuplesBeforeJ = 0;
        _load = _numTuplesStored * _numTuplesJoined;
///在迁入joiner进行new Shard时需要下面的实体，reshuffle与joiner统一
        _indexQueue = new LinkedList();
        _currList = new ArrayList<Values>();
//        _oppRelKeyCurrList = new ArrayList<Long>();
        _numCurrTupleJoined = 0l;
        _numTuplesJoined = 0l;
        _oppRelCountQueue = new LinkedList(); ///长度为_oppRelQueLength
//        _oppRelNumQue =  new LinkedList();

//        _numSampleJoinTuples = 0;

//        _joinSampleStep = 30;
//        _storeSampleStep = 30;
    }

    public Shard(String StrShard, boolean win, long winsize, int subindexSize){  ////for joiner
        left = null;
        right = null;
        String []sh = StrShard.split(",");
        this._shardID = Long.parseLong(sh[0]);
        this._minRange = Long.parseLong(sh[1]);
        this._maxRange = Long.parseLong(sh[2]);
        this._owner = Long.parseLong(sh[3]);
        this._state = Integer.parseInt(sh[4]);
        this._window = win;
        this._thisWinSize = winsize;
        this._subindexSize = subindexSize;
        this._statistics = "";
        _numTuplesStored = 0l;
        _numTuplesBeforeJ = 0;
        _load = _numTuplesStored * _numTuplesJoined;
//        this.storedTuples = new LinkedList<>();
        _indexQueue = new LinkedList();
        _currList = new ArrayList<Values>();
        _numCurrTupleJoined = 0l;
        _numTuplesJoined = 0l;
        _oppRelCountQueue = new LinkedList();

//        _numSampleJoinTuples = 0;

//        _joinSampleStep = 30;
//        _storeSampleStep = 30;
//
        String prefix = "sharder_" + _shardID;
//        _output = new FileWriter("/home/shuiyinyu/join/tmpresult-fastjoin", prefix, "csv");//..setPrintStream(System.out);
//        output("Shard(String)=" + this._window + ",thisWinSize=" + this._thisWinSize + "\n");
    }

    public Shard(String StrShard, int taskId){ ////for monitorBolt  TODO:_numTuplesStored?
        String []statistic = StrShard.split(",");

        _shardID = Long.parseLong(statistic[0]);
        _minRange = Long.parseLong(statistic[1]);
        _maxRange = Long.parseLong(statistic[2]);
        _owner = taskId;
        _state = Integer.parseInt(statistic[3]);
        _numTuplesStored = Long.parseLong(statistic[4]);
        _numTuplesJoined = Long.parseLong(statistic[5]);
        _numTuplesBeforeJ = 0;
        _load = _numTuplesStored * _numTuplesJoined;
    }

    public Shard(Long shardID, Long minRange, Long maxRange, int owner, Long numTupleStored, Long numTupleJoined){ ////for monitorBolt  TODO: _numTuplesStored?
        _shardID = shardID;
        _minRange = minRange;
        _maxRange = maxRange;
        _owner = owner;
        _state = 0;
        _numTuplesStored = numTupleStored;
        _numTuplesJoined = numTupleJoined;
        _numTuplesBeforeJ = 0;
        _load = _numTuplesStored * _numTuplesJoined;
    }

    public String encodeShardInShuffle(){
        String str = null;

        _numTuplesBeforeJ = 0;
        str = String.valueOf(this._shardID) + "," + String.valueOf(this._minRange) + "," + String.valueOf(this._maxRange)
                + "," + String.valueOf(this._owner) + "," + String.valueOf(this._state) + "," + this._statistics;
        return str;
    }

    public String encodeShardInJoiner(){
        String str = null;
        str = String.valueOf(_shardID) + "," + String.valueOf(_minRange) + "," + String.valueOf(_maxRange)
                + "," + String.valueOf(_state) + "," + String.valueOf(_numTuplesStored) + "," + String.valueOf(_numTuplesJoined);

        return str;
    }

    public void store(Tuple tuple) {
        String rel = tuple.getStringByField("relation");
        Long ts = tuple.getLongByField("timestamp");
        Long key = tuple.getLongByField("key");
        String value = tuple.getStringByField("value");
        Values values = new Values("tuple", rel, ts, key, value, Long.MIN_VALUE, Long.MIN_VALUE); ///"type", "relation", "timestamp", "key", "value", "boltID", "shardID"
        _currList.add(values);
        if(_currList.size() >= _subindexSize){
            _indexQueue.add(Pair.of(ts, _currList));
            _currList = Lists.newArrayListWithExpectedSize(_subindexSize);
        }
        _numTuplesStored++;
    }

    public void join(Tuple tupleOpp, BasicOutputCollector collector) {
        /* join with archive indexes */
        int numToDelete = 0,  numOppToDelete = 0;
        Long key = tupleOpp.getLongByField("key");
        Long tsOpp = tupleOpp.getLongByField("timestamp");
        for (Pair pairTsIndex : _indexQueue) {
            Long ts = getLong(pairTsIndex.getLeft());
            if (_window && !isInWindow(tsOpp, ts)) {
                ++numToDelete;
                continue;
            }
            join(tupleOpp, (List<Values>)pairTsIndex.getRight(), collector);
        }

        for (int i = 0; i < numToDelete; ++i) {
            List<Values> pollList = (List<Values>)_indexQueue.poll().getRight();
            _numTuplesStored = _numTuplesStored - pollList.size();/////迁出、迁入要重新设计！！！
        }

        /* join with current index */
        join(tupleOpp, _currList, collector);

        _numTuplesJoined ++;
        _numCurrTupleJoined ++;
        if(_numCurrTupleJoined >= _subindexSize){
            _oppRelCountQueue.add(new MutablePair(tsOpp,_numCurrTupleJoined));
            _numCurrTupleJoined = 0L;
        }

        for(Pair pairIndex : _oppRelCountQueue){
            Long ts = getLong(pairIndex.getLeft());
            if(!isInOppWindow(tsOpp, ts)){
                ++numOppToDelete;
            }
        }
        for(int i = 0; i < numOppToDelete; i ++){
            long expiredCount = (long) _oppRelCountQueue.poll().getRight();
            _numTuplesJoined = _numTuplesJoined - expiredCount;
        }

    }

    private void join(Tuple tupleOpp, List<Values> index, BasicOutputCollector collector){
        Long key = tupleOpp.getLongByField("key");
//        _joinedTime += index.size();
        for(Values record : index){
            ///因为去掉了relation，所以这里的key应该是1
            Long keyRec = (Long)record.get(3);
            Long diff = key - keyRec;
            if(Math.abs(diff) < 50.0){ //join successful
                _resultNum++;
//                String value = tupleOpp.getStringByField("value");
//                String storedValue = getString(record.get(2));//String joinedResult = _taskRel.equals("R") ? (storedValue + ";" + value) : (value + ";" + storedValue);
            }
        }
    }

    ////二期工程
    public boolean modifyShard(String leftOrRight, Long minRange, Long maxRange){
        if(leftOrRight.equals("left")){
            _maxRange = maxRange;
            return true;
        }else if(leftOrRight.equals("right")){
            _minRange = minRange;
            return true;
        }
        return false;
    }

    public boolean immigrate(Tuple tuple){
        return true;
    }

    public boolean emmigrate(Tuple tuple){
        return true;
    }

    public List<Long> SortStoreTuple(){
        ArrayList<Long> storeStatis = new ArrayList<>();
        int currSize = _currList.size();
        for(int i = 0; i < currSize/_storeSampleStep; i++){
            if(currSize <= 0)break;
            int random = (int) Math.floor(Math.random() * currSize) % currSize;
            Values values = _currList.get(random);
            Long key = (Long)values.get(3);
            if(key >= _minRange && key <= _maxRange)
            storeStatis.add(key);
        }

        for (Pair pairTsIndex : _indexQueue){
            List<Values> arrayList = (List<Values>) pairTsIndex.getRight();
            int listSize = arrayList.size();
            for(int i = 0; i < listSize/_storeSampleStep; i++){
                int random = (int) Math.floor(Math.random() * listSize) % listSize;
                Values values = (Values) arrayList.get(random);///////
                Long key = getLong(values.get(3));///.getLongByField("key");
                if(key >= _minRange && key <= _maxRange)
                storeStatis.add(key);
            }
            //pairTsIndex.getRight().get(random)
        }
        Collections.sort(storeStatis);
        return storeStatis;
    }

    public long getRangeCountTuple(long minRangeC, long maxRangeC){
        double count = 0.0;
        output("in getRangeCountTuple(l,l). minRangeC=" + minRangeC + ", maxRangeC=" + maxRangeC + "\n");
        if(minRangeC >= maxRangeC) return 0l;
        if(_maxRange >= _minRange) return 0l;
        double ratio = 0.0;
        if(maxRangeC == 8200) {
            ratio = (double)(maxRangeC - minRangeC)/(8165 - _minRange);
        }else{
            ratio = (double)(maxRangeC - minRangeC)/(_maxRange - _minRange);
        }

/*        for(Pair pairTsIndex: _oppRelCountQueue){
            count = count + (long) pairTsIndex.getRight();
        }
        output("count = " + (long)(count * ratio) + "\n");*/
        return (long)(_numTuplesJoined * ratio);
    }

/*    public List<Long> SortJoinTuple(){
        ArrayList<Long> joinStatis = new ArrayList<>();

        for(int i = 0; i < _oppRelKeyCurrList.size(); i++){
            Long key = _oppRelKeyCurrList.get(i);
            if(key >= _minRange && key <= _maxRange)
            joinStatis.add(key);
        }


        Collections.sort(joinStatis);
        return joinStatis;
    }*/

    private boolean isInWindow(long tsIncoming, long tsStored) {
        if(Math.abs(tsIncoming - tsStored) <= _thisWinSize)return true;
        else {
            return false;
        }
    }

    private boolean isInOppWindow(long tsIncoming, long tsStored) {
        if(Math.abs(tsIncoming - tsStored) <= _thisWinSize/2 + 1)return true;
        else {
            return false;
        }
    }

    public long getNumStored(){
        return _numTuplesStored;
    }

    public long getNumJoined(){
        return _numTuplesJoined;
    }

    ///从存储元组中找到这个范围的元组，发送到迁入task-shard
    public long immigrateTuples(int immTaskID, Long immigrateShardID, String leftOrRight, Long cursor, BasicOutputCollector collector){
        long numMigrateTuple = 0; ////
        int checkedIndex = 0, uncheckedIndex = 0;
        for(Values record: _currList){
//            boolean emitOrNot = false;
//            uncheckedIndex ++;
            Long keyRec = getLong(record.get(3)); ///TODO:还没测试！！
            if(leftOrRight.equals("left") && (keyRec < cursor)){
                collector.emitDirect(immTaskID, JOINER_TO_JOINER_STREAM_ID, record);
                numMigrateTuple ++;
                _numTuplesStored --;
            }else if(leftOrRight.equals("right") && (keyRec >= cursor)){
                collector.emitDirect(immTaskID, JOINER_TO_JOINER_STREAM_ID, record);
                numMigrateTuple ++;
                _numTuplesStored --;
            }else{
                _currList.set(checkedIndex, _currList.get(uncheckedIndex));
                checkedIndex ++;
            }
            uncheckedIndex ++;
        }
        for(int i = _currList.size() - 1; i >= checkedIndex; i--){
            _currList.remove(i);
        }

        for (Pair pairTsIndex : _indexQueue) {
            ArrayList<Values> currList = (ArrayList<Values>)pairTsIndex.getRight();
            checkedIndex = 0;
            uncheckedIndex = 0;
            for(Values record : currList){
                Long keyRec = (Long)record.get(3);
                if(leftOrRight.equals("left") && (keyRec < cursor)){
                    collector.emitDirect(immTaskID, JOINER_TO_JOINER_STREAM_ID, record);
                    numMigrateTuple ++;
                    _numTuplesStored --;
                }else if(leftOrRight.equals("right") && (keyRec >= cursor)){
                    collector.emitDirect(immTaskID, JOINER_TO_JOINER_STREAM_ID, record);
                    numMigrateTuple ++;
                    _numTuplesStored --;
                }else { ////不需要emit的元组保留
                    currList.set(checkedIndex, currList.get(uncheckedIndex));
                    checkedIndex ++;
                }
                uncheckedIndex ++;
            }
            for(int i = currList.size() - 1; i >= checkedIndex; i--){
                currList.remove(i);
            }
        }

//        ///TODO:opp的迁出元组的数量。
////        long numOppRelKey = 0l;
//        double ratio = 0.0;
//        if(leftOrRight.equals("left")){
//            ratio = (double)(cursor - _minRange)/(_maxRange - _minRange);
//        }else if(leftOrRight.equals("right")) {
//            ratio = (double)(_maxRange - cursor)/(_maxRange - _minRange);
//        }
//        long numOppRelKey = (long)(_numCurrTupleJoined * ratio);
//        Long ts = System.currentTimeMillis();
//        Values values = new Values("imJoinT", "opp", ts, numOppRelKey, " ", Long.MIN_VALUE, immigrateShardID);
//        collector.emitDirect(immTaskID, JOINER_TO_JOINER_STREAM_ID, values);////TODO:这个只是一个值，做个什么type？
//        for (Pair pairTsIndex : _oppRelCountQueue){
//            Long ts1 = (Long)pairTsIndex.getLeft();
//            long countOneNode = (long)pairTsIndex.getRight();
//            long numOppRelKey1 = (long)(countOneNode * ratio);
//            Values values1 = new Values("imJoinT", "opp", ts1, numOppRelKey1, " ", Long.MIN_VALUE, immigrateShardID);
//            collector.emitDirect(immTaskID, JOINER_TO_JOINER_STREAM_ID, values1);////TODO:这个只是一个值，做个什么type？
//        }

        ///"type", "relation", "timestamp", "key", "value", "boltID", "shardID"
        ///返回迁出存储元组的数量
        return numMigrateTuple;
    }

    public long subJoinedTupleNum(int immTaskID, long immigrateShardID, long numMigrateTuple, BasicOutputCollector collector){
        ///TODO:opp的迁出元组的数量。
        long migrateJoinedTupleCount = 0l;
        if(_numTuplesStored == 0 || numMigrateTuple == 0) return migrateJoinedTupleCount;;
        double ratio = (double)numMigrateTuple/(_numTuplesStored +  numMigrateTuple);
        long migrateNum = 0;

        Long ts = System.currentTimeMillis();
        migrateNum = (long)(_numCurrTupleJoined * ratio);
        Values values = new Values("imJoinT", "opp", ts, migrateNum, " ", Long.MIN_VALUE, immigrateShardID);
        collector.emitDirect(immTaskID, JOINER_TO_JOINER_STREAM_ID, values);////TODO:这个只是一个值，做个什么type？
        migrateJoinedTupleCount += migrateNum; _numCurrTupleJoined -= migrateNum;
        //"type", "nouse", "timestamp", "key", "nouse", "nouse", "shardID"

        for (Pair pairTsIndex : _oppRelCountQueue){
            Long ts1 = (Long)pairTsIndex.getLeft();
            long countOneNode = (long)pairTsIndex.getRight();
            migrateNum = (long)(countOneNode * ratio);
            Values values1 = new Values("imJoinT", "opp", ts1, migrateNum, " ", Long.MIN_VALUE, immigrateShardID);
            collector.emitDirect(immTaskID, JOINER_TO_JOINER_STREAM_ID, values1);////TODO:这个只是一个值，做个什么type？
            migrateJoinedTupleCount += migrateNum; pairTsIndex.setValue(countOneNode - migrateNum);
        }
        _numTuplesJoined -= migrateJoinedTupleCount;
        return migrateJoinedTupleCount;
    }

    public void addImmJoinTupleNum(long ts, long numImmJoinTuple){
        for(Pair pairTsIndex : _oppRelCountQueue){
            if(ts <= (long)pairTsIndex.getLeft()){
                ////TODO:需要把队列中的,是getValue还是getRight？
                pairTsIndex.setValue((long)pairTsIndex.getRight() + numImmJoinTuple);
                break;
            }
        }
        _numTuplesJoined = _numTuplesJoined + numImmJoinTuple;
    }

    public long countTuples(){
        long numtuples = 0l;
        numtuples += _currList.size();

        for (Pair pairTsIndex : _indexQueue){
            List<Values> arrayList = (List<Values>) pairTsIndex.getRight();
            long listSize = arrayList.size();
            numtuples += listSize;
        }
        return numtuples;
    }

/*    public long overRangeKeys(long minRange, long maxRange){
        long numTuples = 0l;
        for(Long keyRec: _oppRelKeyCurrList){
            if(keyRec < minRange && keyRec > minRange - ){
                numTuples ++;
            }else if(keyRec > maxRange){
                numTuples ++;
            }
        }

        for (Pair pairTsIndex : _oppRelQueue){
            List<Long> arrayList = (List<Long>)pairTsIndex.getRight();
            for(Long keyRec: arrayList){
                if(keyRec < minRange){
                    numTuples ++;
                }else if(keyRec > maxRange){
                    numTuples ++;
                }
            }
        }

        return numTuples * _joinSampleStep;
    }*/

//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declareStream(JOINER_TO_JOINER_STREAM_ID,new Fields(SCHEMA));
//    }

    // +createShard()//SD创建
// -modifyShard()//SD变更
// -deleteShard()//SD注销
// +decodeToShard()//SD解码
// +encodeShard()//SD编码
// -*profitCompute()//计算收益    ###这个应该在ShardAbstract,即joiner
// +store()         //        ######store,join感觉放到jonier好一些
// +join()
// -immigrate()//迁入元组
// -*emmigrate()//迁出元组
    private void output(String msg) {
        if (_output != null){
            //_output.write(msg);
            _output.writeImmediately(msg);
        }
    }
}
