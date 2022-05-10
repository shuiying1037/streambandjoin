package soj.biclique.router;

public class ShardNode<T extends Comparable<T>>{
    ShardNode left;
    ShardNode right;
    public long shardID;

    public T minRange;
    public T maxRange;
    public long owner;

    public long storeNum;
    public long joinNum;
    public long load;

    public ShardNode(long shardID, T minRange, T maxRange, long owner){
        this.left = null;
        this.right = null;
        this.shardID = shardID;
        this.minRange = minRange;
        this.maxRange = maxRange;
        this.owner = owner;
        this.storeNum = 0;
        this.joinNum = 0;
        this.load = storeNum * joinNum;
    }
    public ShardNode(long shardID, T minRange, T maxRange, long owner, long storeNum, long joinNum){
        this.left = null;
        this.right = null;
        this.shardID = shardID;
        this.minRange = minRange;
        this.maxRange = maxRange;
        this.owner = owner;
        this.storeNum = storeNum;
        this.joinNum = joinNum;
        this.load = storeNum * joinNum;
    }
    public ShardNode(ShardNode left, ShardNode right, long shardID, T minRange, T maxRange, long owner, long storeNum, long joinNum){
        this.left = left;
        this.right = right;
        this.shardID = shardID;
        this.minRange = minRange;
        this.maxRange = maxRange;
        this.owner = owner;
        this.storeNum = storeNum;
        this.joinNum = joinNum;
        this.load = storeNum * joinNum;
    }
}