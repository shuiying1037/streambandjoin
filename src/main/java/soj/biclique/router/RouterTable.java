package soj.biclique.router;

import org.apache.storm.shade.clout.core.Route;
import soj.util.FileWriter;

import java.*;
import java.util.*;
public class RouterTable<K extends Comparable<K>> {
    private final int defaultRouterTableSize = 10;
    private ArrayList<RouterItem<K>> _routerTable;
    private int _itemNums;
    FileWriter _output;

    public RouterTable(){
        this._routerTable = new ArrayList<>(defaultRouterTableSize);
        this._itemNums = 0;
        String prefix = "RouterTable_"+ Math.random() * 100;
//        new FileWriter("/home/shuiyinyu/join/tmpresult-fastjoin", prefix, "csv").setFlushSize(1);
    }

    public RouterTable(int defaultSize){
        if(defaultSize >= 0 && defaultSize <= 65536)
            this._routerTable = new ArrayList<>(defaultSize);
        else
            this._routerTable = new ArrayList<>(defaultRouterTableSize);
        this._itemNums = 0;
    }

    public int getItemNums(){
        return this._itemNums;
    }

    public List<RouterItem<K>> getItems(){
        return _routerTable;
    }

    private void sortRouterTable() {
        Collections.sort(_routerTable, new Comparator<RouterItem<K>>() {
            @Override
            public int compare(RouterItem<K> a, RouterItem<K> b) {
                if (a.start.compareTo(b.start) > 0) {
                    return 1;
                } else {
                    return -1;
                }
            }
        });
    }

    public boolean addItem(RouterItem item) {
        //默认左闭右开区间,合法性检查start<=end,shardID不能重复,start不能重复,区间不能重叠
        if (item.start.compareTo(item.end) > 0) {
            return false;
        }
        for (int i = 0; i < _routerTable.size(); i++) {
            if (_routerTable.get(i).shard == item.shard) {
                return false;
            }
            if (item.start.compareTo(_routerTable.get(i).start) == 0) {
                return false;
            }
            if (item.start.compareTo(_routerTable.get(i).start) < 0 &&
                    item.end.compareTo(_routerTable.get(i).start) > 0) {
                return false;
            }
            if (item.start.compareTo(_routerTable.get(i).start) > 0 &&
                    item.start.compareTo(_routerTable.get(i).end) < 0) {
                return false;
            }
        }
        this._routerTable.add(item);
        this.sortRouterTable();
        this._itemNums++;
        return true;
    }

    public RouterItem deleteItem(long shardId) {
        RouterItem deleted = null;
        for (int i = 0; i < _routerTable.size(); i++) {
            if (_routerTable.get(i).shard == shardId) {
                deleted = _routerTable.remove(i);
//                _routerTable.get(i);
                this._itemNums--;
                //this.sortRouterTable();
                break;
            }
        }
        return deleted;
    }

    public boolean modifyRouterTable(long shardIdLeft, long shardIdRight, K newStartEnd, StringBuilder debugString) {
        //检查参数
        if(shardIdLeft == 0 || shardIdRight == 0 || shardIdLeft == shardIdRight)  return false;
        //查找左边item
        for (int i = 0; i < _routerTable.size(); i++){
             if(_routerTable.get(i).shard == shardIdLeft){
                 //检查左边shard右邻
                 if(i+1 >= _routerTable.size()) return false;
                 if(_routerTable.get(i+1).shard != shardIdRight) return false;
                 //检查newStartEnd介于两个范围之间
                 if(_routerTable.get(i).start.compareTo(newStartEnd) > 0
                 || _routerTable.get(i+1).end.compareTo(newStartEnd) < 0)
                     return false;
                 if(_routerTable.get(i).start.compareTo(newStartEnd) == 0){ ///如果左边shard的右界等于新分位点，修改下一个shard的分位点，该shard删除。
                     _routerTable.get(i+1).start = newStartEnd;
                     _routerTable.remove(i);
                     debugString.append("remove left shard." + "\n");
                     return true;
                 }
                 else if(_routerTable.get(i+1).end.compareTo(newStartEnd) == 0) {///如果右边shard的左界等于新分位点，修改前一个shard的分位点，该shard删除。
                     _routerTable.get(i).end = newStartEnd;
                     _routerTable.remove(i+1);
                     debugString.append("remove right shard." + "\n");
                     return true;
                 }
                 else {
                     //设置新的分位点
                     _routerTable.get(i).end = newStartEnd;
                     _routerTable.get(i+1).start = newStartEnd;
                     return true;
                 }

            }
        }
        return false;
    }


    public boolean modifyNShardRouterTable(long emigrateShardId, long newShardId, int newShardTasksId, K newShardStart, K newShardEnd) { ///emiShardID, immShardID, immTaskID, minRange, cursor
        if(newShardStart.compareTo(newShardEnd) == 0){ ///因新建shard边界相等，不新建shard
            return true;
        }
        RouterItem split = deleteItem(emigrateShardId);
        if(split == null)   return false;
        if(split.start.compareTo(newShardStart) == 0 && split.end.compareTo(newShardEnd) == 0){ /////完整迁移，更换shardID
            long oldShard = split.shard;
            int  oldTask = split.task;
            split.task = newShardTasksId;
            split.shard = newShardId;
            if(!addItem(split)){ ////回退。
                split.task = oldTask;
                split.shard = oldShard;
                addItem(split);
                return false;
            }
            return true;
        }
        K oldSplitStart = (K)split.start;
        split.start = newShardEnd;
        if(!addItem(split)){ ////把修改完迁出的RouterItem插回。
            split.start = oldSplitStart;
            addItem(split);
            return false;
        }
        ////构造RouterItem，插入
        RouterItem<K> newItem = new RouterItem<>();
        newItem.start = newShardStart;
        newItem.end = newShardEnd;
        newItem.task = newShardTasksId;
        newItem.shard = newShardId;
        return addItem(newItem);
    }

    public String getTaskIDs(K oneKey){
        return getTaskIDs(oneKey, oneKey);
    }

    public String getTaskIDs(K start, K end, boolean ... closed) {
        //默认选中区间为左闭右闭
        boolean startClosed = true;
        boolean endClosed = true;
        if(closed.length > 0)   startClosed = closed[0];
        if(closed.length > 1)   endClosed = closed[1];

        StringBuilder sb = new StringBuilder();
        if (_routerTable.size() <= 0) return sb.toString();//路由表为空
        if (start.compareTo(end) > 0) return sb.toString();//选中范围无效
        if(!(startClosed == true && endClosed == true) && start.compareTo(end) == 0)
            return sb.toString();//选中范围无效
        //查找范围返回
        //二分查找一个有overlap的记录
        int lo = 0;
        int hi = _routerTable.size() - 1;
        int mid = -1;
        boolean found = false;
        while (lo <= hi) {
            mid = (lo + hi) / 2;
            final K midVal = _routerTable.get(mid).start;
            if (hasOverlap(start, end, endClosed, _routerTable.get(mid))){
                found = true;
                break;
            }
            else if (midVal.compareTo(start) < 0) {
                lo = mid + 1;
            } else if (midVal.compareTo(start) > 0) {
                hi = mid - 1;
            }
        }
        if (!found) return sb.toString();
        else {
            //向记录两侧查找
            List<Integer> map = new ArrayList<>(3);
            Integer taskID = _routerTable.get(mid).task;
            map.add(taskID);
            sb.append(taskID);
            int i = mid + 1;
            while (i < _routerTable.size()) {
                if (hasOverlap(start, end, endClosed, _routerTable.get(i))) {
                    taskID = _routerTable.get(i).task;
                    if(!map.contains(taskID)){
                        map.add(taskID);
                        sb.append(",");
                        sb.append(taskID);
                    }
                }
                else break;
                i++;
            }
            i = mid - 1;
            while (i >= 0) {
                if (hasOverlap(start, end, endClosed, _routerTable.get(i))) {
                    taskID = _routerTable.get(i).task;
                    if(!map.contains(taskID)){
                        map.add(taskID);
                        sb.append(",");
                        sb.append(taskID);
                    }
                }
                else break;
                i--;
            }
        }
        return sb.toString();
    }

    private boolean hasOverlap(K start, K end, boolean endClosed, RouterItem<K>item){
        final K itemStart = item.start;
        final K itemEnd = item.end;
        if(endClosed == true)   // [,] (,]
            return !(end.compareTo(itemStart) < 0 || start.compareTo(itemEnd) >= 0);
        else    // [,) (,)
            return !(end.compareTo(itemStart) <= 0 || start.compareTo(itemEnd) >= 0);
    }

    public static List<Integer> stringToTaskIDs(String str){
        String[] taskIDStrings= str.split(",");
        List<Integer> taskIDs = new ArrayList<>(taskIDStrings.length);
/*        if(str.length() <=0 ){
            return taskIDs;
        }*/
//        if(str.length()==0)return taskIDs;
        for (String x: taskIDStrings) {
            taskIDs.add(Integer.parseInt(x));
        }
        return taskIDs;
    }

    public String printRouter(String str){
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("-----" + str + "-----" + "\n");
/*        for (int i = 0; i < _routerTable.size(); i++) {
            strBuilder.append("[" + _routerTable.get(i).start + ","
                    + _routerTable.get(i).end + ") "
                    + "SD: " + _routerTable.get(i).shard
                    + " tsk: " + _routerTable.get(i).task + "\n");
        }*/

        for (int i = 0; i < _routerTable.size(); i++) {
            if(i == 0)strBuilder.append(((Long)_routerTable.get(i).end) + ",");
            else if((i+1) == _routerTable.size())strBuilder.append((8200L-(Long)_routerTable.get(i).start) + ",");
            else
            strBuilder.append(((Long)_routerTable.get(i).end - (Long)_routerTable.get(i).start) + ",");
        }
        strBuilder.append("\n");
        return strBuilder.toString();
    }

    private void output(String msg) {
        if (_output != null){
            //_output.write(msg);
            _output.writeImmediately(msg);
        }
    }
////12.11新写的
//    public void modifyRouterTable(String modifyOrShard, Long emiShardID, Long immTaskID, Long immShardID, Long minRange, Long cursor,Long  maxRange){

/*        public static void main(String[] args) {
        RouterTable<Long> testTable = new RouterTable<>();
        System.out.println("addItem");
        RouterItem<Long> testItem1 = new RouterItem<>();
        testItem1.start = 0L;
        testItem1.end = 10L;
        testItem1.shard = 11L;
        testItem1.task = 11;
        testTable.addItem(testItem1);

        RouterItem<Long> testItem2 = new RouterItem<Long>();
        testItem2.start = 10L;
        testItem2.end = 20L;
        testItem2.shard = 22L;
        testItem2.task = 22;
        testTable.addItem(testItem2);

        RouterItem<Long> testItem3 = new RouterItem<Long>();
        testItem3.start = 20L;
        testItem3.end = 30L;
        testItem3.shard = 33L;
        testItem3.task = 33;
        testTable.addItem(testItem3);

        RouterItem<Long> testItem4 = new RouterItem<Long>();
        testItem4.start = 30L;
        testItem4.end = 40L;
        testItem4.shard = 44L;
        testItem4.task = 44;
        testTable.addItem(testItem4);

        for (int i = 0; i < testTable._routerTable.size(); i++) {
            System.out.println("[" + testTable._routerTable.get(i).start + ","
                +testTable._routerTable.get(i).end + ") "
                +"SD: " +testTable._routerTable.get(i).shard
                +" tsk: "+testTable._routerTable.get(i).task);
        }

        System.out.println("deleteItem");
        testTable.deleteItem(22);
        for (int i = 0; i < testTable._routerTable.size(); i++) {
            System.out.println("[" + testTable._routerTable.get(i).start + ","
                    +testTable._routerTable.get(i).end + ") "
                    +"SD: " +testTable._routerTable.get(i).shard
                    +" tsk: "+testTable._routerTable.get(i).task);
        }
        testTable.addItem(testItem2);


        System.out.println("getTaskIDs");
        String res = testTable.getTaskIDs(25L, 35L, true, false);
        System.out.println(res);

    }*/

}