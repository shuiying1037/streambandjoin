package soj.biclique.core;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import soj.util.FileWriter;
import soj.util.Stopwatch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AverageGrouping implements CustomStreamGrouping, Serializable {
    private List<Integer> _targetTasks;
    private HashFunction h = Hashing.murmur3_128(13);
    private FileWriter _output;
    private long _lastOutputTime;
    private Stopwatch _stopwatch;
    private long _hashTimes;
    private long _routerTimes;
    public AverageGrouping(){
        _hashTimes = 0;
        _routerTimes = 0;
    }
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        _targetTasks = targetTasks;
        _stopwatch = Stopwatch.createStarted();
        _lastOutputTime = _stopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>(1);
        if(values.size() > 0){
            String skey = values.get(2).toString();
            Long key = Long.parseLong(skey);
            Integer target = (Integer)values.get(4);
            //hash
            if(target == -1){
//                int idx = (int) (Math.abs(h.hashBytes(key.getBytes()).asLong()) % this._targetTasks.size());
                int idx = 0;
                boltIds.add(_targetTasks.get(idx));
                _hashTimes++;
            }
            else{
                target = target % this._targetTasks.size();
                boltIds.add(target);
                _routerTimes++;
            }
        }
        return boltIds;
    }
}

//        long _currTime = _stopwatch.elapsed(TimeUnit.MILLISECONDS);;
//        if(_currTime > _lastOutputTime + 5000){
//           _output.writeImmediately("Hash times: " + _hashTimes + " router times: " + _routerTimes);
//        }