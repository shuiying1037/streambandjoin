package soj.biclique.router;

import soj.biclique.Shard.Shard;

import java.util.*;
import java.lang.*;
import soj.util.FileWriter;

/*
* Joiner负载汇报修改MonitorTable的API
* setOrCreatShard(Shard SD);
* 输入要求:SD包含owner,shardid,load
* 功能描述:只更新load,创建shard但不接入值域链
*
* 负载迁移结束后对MonitorTable操作的API
* 负载迁移结束有3类完全情况
* 1、迁出task创建了一个shard发送给迁入task
* 2、迁出task将一个shard部分负载迁移至相连的shard
* 3、迁出task将shard负载完全迁空并注销shard
* 4*、迁出task创建相连的shard发送给迁入task,将负载完全迁空后注销原shard
* *第4类情况可由1和3组合完成
*所有操作归结到MonitorTable三个API
* 1、修改两个shard之间的分位点 setLeftQuantile(Shard s, long newLeftQuantile);
* setRightQuantile(Shard s, long newRightQuantile);
* 2、为一个shard新设置左右分位点 addShardToRangeList(Shard SD, long leftQuantile, long rightQuantile);
* 3、注销一个shard,并应用一个新的分位点 deleteShard(Shard toDelete);
* *初始化需循环使用addShardToRangeList API？
*
* 获取task邻居的API
* public String getNbToString(long taskID);
* 返回字符串的格式为
* Shard1.encode(),Shard1.owner.taskLoad;
* Shard1.left.encode(),Shard1.left.owner.taskLoad;
* Shard1.right.encode(),Shard1.right.owner.taskLoad;
* Shard2.encode(),Shard1.owner.taskLoad;
* Shard2.left.encode(),Shard1.left.owner.taskLoad;
* Shard2.right.encode(),Shard1.right.owner.taskLoad;
* ...
* */
public class MonitorTable{
    public class TaskNode{
        HashMap<Long, Shard> _shardList;
        public long _taskID;
        public int _taskShardNum;
        public long _taskLoad;
        public double _miu;
        public double _lamda;
        ///12.6加的
//        public ArrayList<Long> _numStoredT;
//        public ArrayList<Long> _numJoinedT;

        public TaskNode(long taskID){
            _shardList = new HashMap<>(1);
            _taskID = taskID;
            _taskShardNum = 0;
            _taskLoad = 0;
            _miu = 0.0;
//            _lamda = 0.0;
            ///12.6加的
//            _numStoredT = new ArrayList<Long>(_taskShardNum);
//            _numJoinedT = new ArrayList<Long>(_taskShardNum);
        }
    }

    HashMap<Long,TaskNode> _taskTable;

    Shard _theLeftShard;
    Shard _theRightShard;

    public int _taskNum;
    public int _shardNum;
    public long _totalLoad;////这个与_taskLoad有什么区别？
    FileWriter _output;

    public MonitorTable(int maxTaskNum){
        _theLeftShard = new Shard(0l, Long.MIN_VALUE, Long.MIN_VALUE, 0);
//        _theLeftShard._shardID = 0;
        _theRightShard = new Shard(0l, Long.MAX_VALUE, Long.MAX_VALUE, 0);
        _theRightShard._shardID = 0;
        _theLeftShard.left = null;
        _theLeftShard.right = _theRightShard;
        _theRightShard.left = _theLeftShard;
        _theRightShard.right = null;
        _taskNum = 0;
        _shardNum = 0;
        _totalLoad = 0;
        _taskTable = new HashMap<Long,TaskNode>(maxTaskNum);
        String prefix = "monitorTable_"+ Math.random() * 100;
        _output = new FileWriter("/home/shuiyinyu/join/tmpresult-fastjoin", prefix, "csv");
    }

    public TaskNode creatTask(long taskID){ //如果不存在,创建新的TaskNode并返回
        if(!_taskTable.containsKey(taskID)){
            _taskTable.put(taskID,new TaskNode(taskID));
            _taskNum++;
        }
        return _taskTable.get(taskID);
    }

    public boolean setTaskMiu(long taskId, double miu){
        if(getTask(taskId) != null){
            getTask(taskId)._miu = miu;
            return true;
        }
        return false;
    }

    public double getTaskMiu(long taskId){
        if(getTask(taskId) != null){
            return getTask(taskId)._miu;
        }
        return 0.0;
    }

    public double getTaskLamda(long taskId){
        if(getTask(taskId) != null){
            return getTask(taskId)._lamda;
        }
        return 0.0;
    }

    public Shard setOrCreatShard(Shard SD){ //monitor接受load_report,创建或修改TaskNode、Shard并设置负载
        long taskID = SD._owner;
        long shardID = SD._shardID;
        TaskNode tsk = creatTask(taskID);
        if(tsk._shardList.containsKey(shardID)){
            //修改
            Shard shd = tsk._shardList.get(shardID);
            shd._numTuplesStored = SD._numTuplesStored;
            shd._numTuplesJoined = SD._numTuplesJoined;
            tsk._taskLoad -= shd._load;
            _totalLoad -= shd._load;
            //tsk._shardList.put(shardID, SD);
            SD._load = SD._numTuplesStored * SD._numTuplesJoined;
            tsk._taskLoad += SD._load;
            _totalLoad += SD._load;
            shd._load = SD._load;
            return tsk._shardList.get(shardID);
        }
        else{
            //添加
            SD.left = null;
            SD.right = null;
            tsk._shardList.put(shardID, SD);
            tsk._taskLoad += SD._load;
            tsk._taskShardNum++;
            SD._load = SD._numTuplesStored * SD._numTuplesJoined;
            _totalLoad += SD._load;
            _shardNum++;
            return tsk._shardList.get(shardID);
        }
    }

    public Shard getShard(long taskID, long shardID){ //查找返回Shard
        if(_taskTable.containsKey(taskID)) {
            if(_taskTable.get(taskID)._shardList.containsKey(shardID)){
                return _taskTable.get(taskID)._shardList.get(shardID);
            }
            else return null;
        }
        else return null;
    }

    public boolean getShardOrN(long taskID){ //排除错误，打印
        if(_taskTable.containsKey(taskID)) {
//            if(_taskTable.get(taskID)._shardList.containsKey(shardID)){
                return true;
//            }
//            else return false;
        }
        else return false;
    }

    public TaskNode getTask(long taskID){ //找返回TaskNode
        if(_taskTable.containsKey(taskID)) {
            return _taskTable.get(taskID);
        }
        else return null;
    }

    public Long getTaskLoad(long taskID){ ///12.7加的 返回task的负载和
        if(_taskTable.containsKey(taskID)) {
            TaskNode taskNode = _taskTable.get(taskID);
            return taskNode._taskLoad;
        }
        else return 0L;
    }

    private Long getTotalJoinTuple(long taskID){
        Long totalJoinTuple = 0L;
        if(_taskTable.containsKey(taskID)) {
            TaskNode tasknode = _taskTable.get(taskID);
            for (Shard shard : tasknode._shardList.values()) {
                totalJoinTuple += shard._numTuplesJoined;
            }
        }
        return totalJoinTuple;
    }
    private Long getTotalStoredTuple(long taskID){
        Long totalStoreTuple = 0L;
        if(_taskTable.containsKey(taskID)) {
            TaskNode tasknode = _taskTable.get(taskID);
            for (Shard shard : tasknode._shardList.values()) {
                totalStoreTuple += shard._numTuplesStored;
            }
        }
        return totalStoreTuple;
    }

    public String getTaskToString(long taskID){ //以字符串返回task的id,shard数量,负载和
        if(_taskTable.containsKey(taskID)) {
            TaskNode tasknode = _taskTable.get(taskID);
            Long totalJoinTuple = 0L, totalStoreTuple = 0L;
            totalJoinTuple = getTotalJoinTuple(taskID);
            totalStoreTuple = getTotalStoredTuple(taskID);

            return String.valueOf(tasknode._taskID) + ","
                    + String.valueOf(tasknode._taskShardNum) + ","
                    + String.valueOf(tasknode._taskLoad) + ","
                    + String.valueOf(tasknode._miu) + ","
                    + String.valueOf(totalJoinTuple) + ","
                    + String.valueOf(totalStoreTuple);
        }
        else return null;
    }

//    public String enumerateLoad() { //返回最大最小负载TaskNode
//        String loadss = "";
//
//        for (TaskNode t : _taskTable.values()) {
//            loadss = loadss + "," + t._taskID + "," + t._taskLoad;
//        }
//        return loadss;
//    }

    public boolean setLeftQuantile(Shard sha, long newLeftQuantile){ ////设置左界，如果左界和原shard的右界相等，删除shard
        if(sha == null) return false;
        if(_taskTable.containsKey(sha._owner)
                && _taskTable.get(sha._owner)._shardList.containsKey(sha._shardID)){
            Shard innerShard = _taskTable.get(sha._owner)._shardList.get(sha._shardID);
            if(newLeftQuantile > innerShard._maxRange)  return false;
            if(newLeftQuantile == innerShard._maxRange){
                return deleteShard(innerShard);
            }
            if(innerShard.left != null && newLeftQuantile < innerShard.left._maxRange)
                return false;
            innerShard._minRange = newLeftQuantile;
            return true;
        }
        else return false;
    }

    public boolean setRightQuantile(Shard s, long newRightQuantile){ ////设置右界，如果右界和原shard的左界相等，删除shard
        if(s == null)   return false;
        if(_taskTable.containsKey(s._owner)
                && _taskTable.get(s._owner)._shardList.containsKey(s._shardID)){
            Shard innerShard = _taskTable.get(s._owner)._shardList.get(s._shardID);
            if(newRightQuantile < innerShard._minRange)  return false;
            if(newRightQuantile == innerShard._minRange){
                return deleteShard(innerShard);
            }
            if(innerShard.right != null && newRightQuantile > innerShard.right._minRange)
                return false;
            innerShard._maxRange = newRightQuantile;
            return true;
        }
        else return false;
    }

    public boolean addShardToRangeList(Shard SD) {
        if(SD == null)   {
            output("addShardToRangeList(), 1#"+"\n");
            return false;
        }
        Long leftQuantile = SD._minRange;
        Long rightQuantile = SD._maxRange;
        Shard itr = _theLeftShard;
        //检索插入新SD的位置
        while(itr != null){
            if(itr._minRange >= SD._minRange) break;
            itr = itr.right;
        }
        if(itr == null || itr.left == null) return false;
        //检查上链合法性
        if(leftQuantile > rightQuantile || rightQuantile > itr._minRange){
            output("addShardToRangeList(), 2#"+"\n" + ",leftQuantile=" + leftQuantile +",rightQuantile=" + rightQuantile + ",itr._minRange=" +itr._minRange + "\n");
            return false;
        }
        if(_taskTable.containsKey(SD._owner) && _taskTable.get(SD._owner)._shardList.containsKey(SD._shardID));
        else {
            SD._load = 0;
            setOrCreatShard(SD);
        }
        Shard innerShard = _taskTable.get(SD._owner)._shardList.get(SD._shardID);
        //如果在链上则失败
        if(innerShard.left != null || innerShard.right != null) {
            output("addShardToRangeList(), 3"+"\n");
            return false;
        }
        //插入迭代器前面
        innerShard._minRange = leftQuantile;
        innerShard._maxRange = rightQuantile;
        if(itr.left._shardID != 0){
            itr.left._maxRange = leftQuantile;
        }
        itr.left.right = innerShard;
        innerShard.left = itr.left;
        if(itr._shardID != 0){
            itr._minRange = rightQuantile;
        }
        itr.left = innerShard;
        innerShard.right = itr;
        //上链不加负载
//        output("Success add to List,SD._minRange=" + SD._minRange + ", SD._maxRange=" + SD._maxRange + "\n");
        return true;
    }

    public boolean deleteShard(Shard toDelete) {
        if(toDelete == null)   return false;
        if(_taskTable.containsKey(toDelete._owner)
                && _taskTable.get(toDelete._owner)._shardList.containsKey(toDelete._shardID))
        {
            Shard toDeleteShard = _taskTable.get(toDelete._owner)._shardList.get(toDelete._shardID);
            //从链上摘下
            if(toDeleteShard.left != null || toDeleteShard.right == null)
                toDeleteShard.left.right = toDeleteShard.right;
            if(toDeleteShard.right != null)
                toDeleteShard.right.left = toDeleteShard.left;
            //销毁Shard
            getTask(toDeleteShard._owner)._taskLoad -= toDeleteShard._load;
            _totalLoad -= toDeleteShard._load;
            getTask(toDeleteShard._owner)._taskShardNum--;
            _shardNum--;
            getTask(toDeleteShard._owner)._shardList.remove(toDeleteShard._shardID);
            return true;
        }
        else return false;
    }

    public String outputLoadMiu(){
        StringBuilder sb = new StringBuilder();
        sb.append("taskID, taskLoad, storedT, miu, totalJoinTuple \n");
        double cons = 181190688.0;
        for(TaskNode t : _taskTable.values()){
            long taskStoreT = 0L;
            for(Shard sha : t._shardList.values()){
                taskStoreT += sha._numTuplesStored;
            }
            sb.append(t._taskID + "," + t._taskLoad + "," + taskStoreT + "," + t._miu + "," + getTotalJoinTuple(t._taskID) + "\n");
        }
        sb.append("------- \n");
        return sb.toString();
    }

    public List<Long> getMaxMinTask(){ //返回最大最小负载TaskNode
        long maxLoad = -1L;
        Long maxNodeID = -1L;
        long minLoad = _totalLoad + 1L;
        Long minNodeID = -1L;

        for(TaskNode t : _taskTable.values()){
            if(t._taskLoad >= maxLoad){
                maxLoad = t._taskLoad;
                maxNodeID = t._taskID;
            }
            if(t._taskLoad <= minLoad){
                minLoad = t._taskLoad;
                minNodeID = t._taskID;
            }
        }
        ArrayList<Long> results = new ArrayList<>(4);
        if(maxNodeID != -1L) {
            results.add(maxNodeID);
            results.add(maxLoad);
        }
        if(minNodeID != -1L) {
            results.add(minNodeID);
            results.add(minLoad);
        }
        return results;
    }


    public StringBuilder getAllTaskLoadString(){ //返回最大最小负载TaskNode
        StringBuilder sb = new StringBuilder();

        for(TaskNode t : _taskTable.values()){
            sb.append(t._taskLoad + "," + t._taskShardNum + ",");
        }
        return sb;
    }

    public String getNetworkDiameter(){
        int diameter = -1, averageDis = -1;
        int sizeTask = _taskTable.size();
        int[][] distance = new int[sizeTask][sizeTask];
        StringBuilder sb = new StringBuilder();
        HashMap<Long, Integer> taskIdToIDX = new HashMap<>();
        int idx = 0;
        /////将taskID转成从0开始的计数值
        output("taskTable size:" + _taskTable.size() + "\n");
        for(TaskNode t : _taskTable.values()){
            output(t._taskID + ",");
            taskIdToIDX.put(t._taskID, idx++);
        }
        /////
        output("\n");
        for(Long taskId : taskIdToIDX.keySet()){
            output(taskId.toString());
        }
        for(int i = 0; i < sizeTask; i++){
            for(int j = 0; j < sizeTask; j++){
                if(i == j) distance[i][j] = 0;
                else
                distance[i][j] = sizeTask + 1;
            }
        }
        output("\n");
        for(TaskNode t : _taskTable.values()){
            for(Shard s : t._shardList.values()){
                int origin = taskIdToIDX.get(t._taskID);
                if(s.left != null && s.left._shardID != 0){
                    int left = taskIdToIDX.get(s.left._owner);
                    if(origin != left){
                        distance[origin][left] = 1;
                        distance[left][origin] = 1;
                    }
                }
                if(s.right != null && s.right._shardID != 0){
                    int right = taskIdToIDX.get(s.right._owner);
                    if(origin != right){
                        distance[origin][right] = 1;
                        distance[right][origin] = 1;
                    }
                }
            }
        }
        for(int k = 0; k < sizeTask; k++){
            for(int i = 0; i < sizeTask; i++){
                for(int j = 0; j < sizeTask; j++){
                    if(distance[i][k] + distance[k][j] < distance[i][j])distance[i][j] = distance[i][k] + distance[k][j];
                }
            }
        }

        for(int i = 0; i < sizeTask; i++){
            for(int j = 0; j < sizeTask; j++){
                if(distance[i][j] <= sizeTask && distance[i][j] > diameter){
                    diameter = distance[i][j];
                }
                averageDis += distance[i][j];
            }
        }
        if(sizeTask != 0)
        averageDis /= (sizeTask*sizeTask);

        sb.append(diameter + ",");
        sb.append(averageDis);

        return sb.toString(); ////TODO
    }

 /*   public int diameter_RegionNet(){
        int diameter = 0;
        for(TaskNode t : _taskTable.values()){
            for(Shard s : t._shardList.values()){
                int tmpDiameter = traverse_BFS_Diameter(s);
                if(tmpDiameter > diameter)diameter = tmpDiameter;
            }
        }
        return diameter;
    }


    // 广度优先遍历，给出图邻接矩阵和开始遍历的节点
    private int traverse_BFS_Diameter(Shard sha) {
        Shard pre = sha;
        if (_taskTable.size() == 0) {
            System.err.println("wrong arcs[][] or begin!");
            return 0;
        }
        HashMap<Long,TaskNode> arcs = _taskTable;
        int num = _taskTable.size();
        boolean[] hasVisit = new boolean[num];
        Queue<Integer> queue = new LinkedList<Integer>();

        hasVisit[begin] = true;
        queue.enQueue(begin);
        int temp, min, n = 0;
        while (!queue.isEmpty()) {
            temp = ( (Integer) queue.deQueue()).intValue();
            for (int i = 1; i < num; i++) {
                // 距离最短的优先入队
                min = Integer.MAX_VALUE;
                for (int j = 0; j < num; j++) {
                    if (!hasVisit[j] && arcs[temp][j] != -1 && arcs[temp][j] < min) {
                        min = arcs[temp][j];
                        n = j;
                    }
                }
                if (min == Integer.MAX_VALUE) {
                    break;
                }
                else {
                    hasVisit[n] = true;
                    queue.enQueue(n);
                    Main.Q_BFS.enQueue(n);
                    Main.length_BFS += Main.arcs[pre][n];
                    pre = n;
                }
            }
        }
    }*/

//    public List<Shard> getNeighbours(long taskID){ //返回TaskNode的所有相邻的ShardNode ---返回是个List<Long>,还要找到相应的存储元组和连接元组
//        ArrayList<Shard> results = new ArrayList<>();
//        if(!_taskTable.containsKey(taskID)){
//            return results;
//        }
//        HashSet<Long> shardIDSet = new HashSet<Long>(_taskNum);
//        for(Shard t : _taskTable.get(taskID)._shardList.values()){
//            if(t.left != null && !shardIDSet.contains(t.left._shardID)){
//                results.add(t.left);
//                shardIDSet.add(t.left._shardID);
//            }
//            if(t.right != null && !shardIDSet.contains(t.right._shardID)){
//                results.add(t.right);
//                shardIDSet.add(t.right._shardID);
//            }
//        }
//        return results;
//    }

    public String getLowLoadNeighboursS(long emiTaskID){ //得到该task的一个最低task负载和邻居的分片
        String str = "";
        Long minNbLoad = Long.MAX_VALUE;
        Shard minLoadShard = null;
        Long immTaskID = 0l;
        boolean isLeftNeighbor = false;
        if(!_taskTable.containsKey(emiTaskID)){
            output("emiTaskID=" + emiTaskID);
            return str;
        }
        long neighborTaskLoad;
        for(Shard t : _taskTable.get(emiTaskID)._shardList.values()){
            if(t.left == null)output("++++++t.left is null" );
            if(t.right == null)output("++++++t.right is null"+ "\n");
            output("emiTaskID=" + emiTaskID + "," + t.encodeShardInJoiner() + "\n");
//            output("++++++right" + t.right.encodeShardInJoiner()+ ",t.right._shardID"+ t.right._shardID + "\n");
            if(t.left != null && t.left._shardID > 0 && t.left._owner != emiTaskID){
                output("in the below if(t.left != null && t.left._shardID > 0)" + "\n");
                neighborTaskLoad = _taskTable.get(t.left._owner)._taskLoad;
                if(minNbLoad > neighborTaskLoad){
                    minLoadShard = t.left;
                    minNbLoad = neighborTaskLoad;
                    immTaskID = minLoadShard._owner;
                    isLeftNeighbor = true;
                }
            }
            if(t.right != null && t.right._shardID > 0 && t.right._owner != emiTaskID){
                output("in the below if(t.right != null && t.right._shardID > 0)" + "\n");
                neighborTaskLoad = _taskTable.get(t.right._owner)._taskLoad;
                if(minNbLoad > neighborTaskLoad){
                    minLoadShard = t.right;
                    minNbLoad = neighborTaskLoad;
                    immTaskID = minLoadShard._owner;
                    isLeftNeighbor = false;
                }
            }
        }
        if(minLoadShard != null){ //indicate: "left" 表示迁入分片在迁出分片的左邻
            String LeftOrNeighbor = isLeftNeighbor? "left" : "right";
            Long emigrateShardID = isLeftNeighbor? minLoadShard.right._shardID : minLoadShard.left._shardID;
            Long immTaskLoad = getTaskLoad(immTaskID);
            str = minLoadShard.encodeShardInJoiner() + "," + emiTaskID + "," + emigrateShardID + "," + LeftOrNeighbor
                    + "," + immTaskID + "," + immTaskLoad + "," + getTaskMiu(immTaskID) + "," + getTotalJoinTuple(immTaskID) + "," + getTotalStoredTuple(immTaskID);
        }
        output("in getLowLoadNeighboursS, str =," + str + "\n");
        return str;
    }

    public void initialList(){

    }

    public void ModifyList(){

    }

    public String outPutAllMonitorTableItems(String str){
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("-------" + str + "-------" + "\n");
        Shard tempShard0 = _theRightShard;
/*        strBuilder.append("-----Output the List,by left---" + str + "\n");
        while (tempShard0 != null){
            strBuilder.append( tempShard0._owner + "," + tempShard0.encodeShardInJoiner() + "," + tempShard0._numTuplesJoined + "," + tempShard0._load + "\n");
            tempShard0 = tempShard0.left;
        }
        strBuilder.append("-------end------" + "\n");

        Shard tempShard = _theLeftShard;
        strBuilder.append("-----Output the List,by right--" + str + "\n");
        while (tempShard != null){
            strBuilder.append( tempShard._owner + "," + tempShard.encodeShardInJoiner() + "," + tempShard._numTuplesJoined + "," + tempShard._load + "\n");
            tempShard = tempShard.right;
        }
        strBuilder.append("-------end------" + "\n");*/
        strBuilder.append("-----Output the List, by task-----" + str + "\n");
        for(TaskNode t: _taskTable.values()){
            strBuilder.append(" " + t._taskID + "\n");
            for(Shard tempShard1: t._shardList.values()){
                strBuilder.append( tempShard1._owner + "," + tempShard1.encodeShardInJoiner() + "," + tempShard1._numTuplesJoined + "," + tempShard1._load + "\n");
            }
        }
        strBuilder.append("-------end------" + "\n");
        return strBuilder.toString();
    }

    private void output(String msg) {
        if (_output != null){
            //_output.write(msg);
            _output.writeImmediately(msg);
        }
    }



//    public String getNbToString(long taskID){ //返回TaskNode的所有相邻的ShardNode ---返回是个List<Long>,还要找到相应的存储元组和连接元组
//        StringBuilder str = new StringBuilder();
//        if(!_taskTable.containsKey(taskID)){
//            return str.toString();
//        }
//        //HashSet<Long> shardIDSet = new HashSet<Long>(_taskNum);
//        int i = 0;
//        for(Shard t : _taskTable.get(taskID)._shardList.values()){
//            if(i > 0)str.append(";");
//            str.append(t.encodeShardInJoiner());
//            str.append(";");
//            if(t.left != null){
//                str.append(t.left.encodeShardInJoiner());
//                str.append(t.left._owner);
//                str.append(getTaskLoad(t.left._owner));
//            }
//            str.append(";");
//            if(t.right != null){
//                str.append(t.right.encodeShardInJoiner());
//                str.append(t.right._owner);
//                str.append(getTaskLoad(t.right._owner));
//            }
//            i++;
//        }
//        return str.toString(); ////左右好像也要区分的好一些:(,不过带了区间的左右值，不区分好像也可以。 最后返回的是：String.valueOf(_shardID) + "," + String.valueOf(_minRange) + "," + String.valueOf(_maxRange)
//        ///+ "," + String.valueOf(_state) + "," + String.valueOf(_numTuplesStored) + "," + String.valueOf(sumnumJoined/5); +"," + ownerID + "," + ownerLoad
//    }

//    public static void main(String[] args) {
//        System.out.println("create");
//        MonitorTable<Long> monitorTable = new MonitorTable<Long>(24);
//        System.out.println("task");
//        for(long i = 1; i < 100; i++)   monitorTable.creatTask(i);
//        System.out.println("taskNum: " + String.valueOf(monitorTable._taskNum));
//        System.out.println("shardNum: " + String.valueOf(monitorTable._shardNum));
//        System.out.println("totalLoad: " + String.valueOf(monitorTable._totalLoad));
//
//        System.out.println("shard");
//        Long a = 0L;
//        Long b = 1L;
//        for(long i = 1588; i < 1788 ; i++){
//            ShardNode<Long>SD = new ShardNode(i, a, b, (long)(Math.random() * 99) + 1, 20, 50);
//            monitorTable.setOrCreatShard(SD);
//        }
//        System.out.println("taskNum: " + String.valueOf(monitorTable._taskNum));
//        System.out.println("shardNum: " + String.valueOf(monitorTable._shardNum));
//        System.out.println("totalLoad: " + String.valueOf(monitorTable._totalLoad));
//        for(long i = 1; i < 100; i++) {
//            System.out.println("-----------------------------------------");
//            System.out.println("_taskID: " + String.valueOf(monitorTable.getTask(i)._taskID));
//            System.out.println("_taskShardNum: " + String.valueOf(monitorTable.getTask(i)._taskShardNum));
//            System.out.println("_taskLoad: " + String.valueOf(monitorTable.getTask(i)._taskLoad));
//        }
//    }

}
