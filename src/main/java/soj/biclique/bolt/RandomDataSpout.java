package com.basic.core.Component;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.utils.Utils;
import soj.util.FileWriter;

public class RandomDataSpout extends BaseRichSpout {

  private static final Logger LOG = LoggerFactory.getLogger(RandomDataSpout.class);
  private final String rel;
  private final String topic;

  private Random random;
  private SpoutOutputCollector spoutOutputCollector;

  private final long tupleRate; // = 1000;
  private long tuplesPerTime;
  private BufferedReader bufferedReader;
  private final int intLower = 0;
  private final int intUpper = 200;
  private final double lngLower = 102.54;
  private final double lngUpper = 104.53;
  private final double latLower = 30.05;
  private final double latUpper = 31.26;
  private final String inputFile;
  private FileWriter _output;
  private final int loopSleep = 100;
  private int loopStart = 0;

  public RandomDataSpout(String rel, String inFile, int tupleR) {
    super();
    this.rel = rel;
    if(rel.equals("R"))topic="Orders2";
    else if(rel.equals("S"))topic="Gps2";
    else topic="Gps3";
    tupleRate = tupleR;
    inputFile = inFile;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    random = new Random();
    spoutOutputCollector = collector;
    tuplesPerTime = tupleRate/100;

    try {
      bufferedReader = new BufferedReader(new FileReader("/home/shuiyinyu/join/didi/" + rel + "/" + inputFile));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      e.printStackTrace();
    }//    _output = new FileWriter("/home/shuiyinyu/join/tmpresult-fastjoin", "Spout--", "txt");
  }

  @Override
  public void nextTuple() {
    for (int i = 0; i < tuplesPerTime; i++) {
      Values values = null;
      String line = null;
      try {
        if ((line = bufferedReader.readLine()) != null) {
          values = new Values(topic, line);
          spoutOutputCollector.emit(values);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    if(loopStart < loopSleep){///逐渐增加吞吐。
      Utils.sleep(15*(loopSleep - loopStart));
      loopStart++;
    }
/*    for(; loopStart < loopSleep; loopStart++){
      Utils.sleep(20*(loopSleep - loopStart));
    }*/
    Utils.sleep(10);
  }

  public Values generateData() {

    String value = "0,1,2,";
    double lng = lngLower + random.nextDouble() * (lngUpper - lngLower);
    double lat = latLower + random.nextDouble() * (latUpper - latLower);
    value += String.format("%.4f,%.4f", lng, lat);
    return new Values(topic, value);

  }

  private void output(String msg) {
    if (_output != null){
      //_output.write(msg);
      _output.writeImmediately(msg);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("topic", "value"));
  }
}
