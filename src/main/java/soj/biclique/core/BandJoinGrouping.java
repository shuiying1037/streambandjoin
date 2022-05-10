package soj.biclique.core;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import soj.biclique.router.RouterTable;
import soj.util.FileWriter;
import soj.util.Stopwatch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
//import soj.biclique.router;

import static soj.util.CastUtils.getLong;

public class BandJoinGrouping implements CustomStreamGrouping, Serializable {
    private List<Integer> _targetTasks;
    private HashFunction h = Hashing.murmur3_128(13);
    private FileWriter _output;
    private long _lastOutputTime;
    private Stopwatch _stopwatch;
    private long _hashTimes;
    private long _routerTimes;
    private long maxrange,minrange;//////////从reshuffle搬过来，在reshuffle还没有去掉，准备只是赋值，没有传参。
//    private long rangeArrR[][], rangeArrS[][];
    private int numPartitionR, numPartitionS;

    /////因流S的数据比R的大50倍，所以以S流的分区为主。 --------要放到monitor里面啊。

    public BandJoinGrouping(){
        _hashTimes = 0;
        _routerTimes = 0;
        minrange = 0L;
        maxrange = 1L<<(13L*2); //8192，
        numPartitionR = 10;
        numPartitionS = 10;
    }

    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        _targetTasks = targetTasks;
        _stopwatch = Stopwatch.createStarted();
        _lastOutputTime = _stopwatch.elapsed(TimeUnit.MILLISECONDS);
        String prefix = "bandjoinGrouping";
        _output = new FileWriter("/home/shuiyinyu/join/tmpresult-fastjoin", prefix, "csv").setFlushSize(10);
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>(1);
        if(values.size() > 0){
//            String key = values.get(3).toString();
            String target = values.get(5).toString();
//            output("target=" + target);
            return  RouterTable.stringToTaskIDs(target);
        }
        return boltIds;
    }
    private void output(String msg) {
        if (_output != null){
            //_output.write(msg);
            _output.writeImmediately(msg);
        }
    }
}

//        long _currTime = _stopwatch.elapsed(TimeUnit.MILLISECONDS);;
//        if(_currTime > _lastOutputTime + 5000){
//           _output.writeImmediately("Hash times: " + _hashTimes + " router times: " + _routerTimes);
//        }