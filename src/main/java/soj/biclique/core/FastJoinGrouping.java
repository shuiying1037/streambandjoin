package soj.biclique.core;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.hash.*;
import soj.util.FileWriter;
import soj.util.Stopwatch;
import static soj.util.CastUtils.getLong;

public class FastJoinGrouping implements CustomStreamGrouping, Serializable {
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
//    int[][] rangeArrR = { {0,451}, {451,861}, {861,1476}, {1476,2050}, {2050,2993}, {2993,4305}, {4305,6519}, {6519,7134}, {7134,7626}, {7626,8200} };
//    int[][] rangeArrS = { {0,451}, {451,861}, {861,1476}, {1476,2050}, {2050,2993}, {2993,4305}, {4305,6519}, {6519,7134}, {7134,7626}, {7626,8200} };
    int[][] rangeArrR = { {0,448}, {448,850}, {850,1380}, {1380,2150}, {2150,3073}, {3093,4210}, {4210,5370}, {5370,6814}, {6814,7400}, {7400,8200} };
//    int[][] rangeArrR = { {0,820}, {820,1640}, {1640,2460}, {2460,3280}, {3280,4100}, {4100,4920}, {4920,5740}, {5740,6560}, {6560,7380}, {7380,8200} };

    public FastJoinGrouping(){
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
        String prefix = "fastjoinGrouping";
        _output = new FileWriter("/home/shuiyinyu/join/tmpresult-fastjoin", prefix, "csv").setFlushSize(10);
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>(1);
        if(values.size() > 0){
            Long key = getLong(values.get(2));
            Integer target = (Integer)values.get(4);
            if(key > maxrange){
                target = numPartitionR-1;
            }
            if(target == -1){ ////假设R与S的处理单元数量相同
                int i = 0;
                for(; i < numPartitionR; i++){
                    if(key < rangeArrR[i][1]){
                        target = i;
                        break;
                    }
                }
                if(i == numPartitionR){ ////key大于所有的数组中的值
                    target = numPartitionR - 1;
                }
                boltIds.add(_targetTasks.get(target));
            }
            else{
                target = target % this._targetTasks.size();
                boltIds.add(_targetTasks.get(target));
                _routerTimes++;
            }
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