package soj.biclique.router;

public class RouterItem<T extends Comparable<T>> {
    public T start;
    public T end;
    public long shard;  /////这个表示什么了？？？？？？如果是shard的ID，为啥只有一个呢？应该有几个啊
    public int  task;
}
