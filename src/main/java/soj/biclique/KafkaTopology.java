package soj.biclique;

import com.esotericsoftware.minlog.Log;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;
import static org.slf4j.LoggerFactory.getLogger;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import soj.biclique.bolt.*;
import soj.biclique.core.BandJoinGrouping;
import soj.biclique.core.ContRandGrouping;
import soj.biclique.core.FastJoinGrouping;
import soj.util.FileWriter;

import static soj.util.LogHelpers.logTopology;
import static soj.util.StormRunner.runInCluster;
import static soj.util.StormRunner.runLocally;

public class KafkaTopology
{
    private static final Logger LOG = getLogger(KafkaTopology.class);
    private static final String SHUFFLE_BOLT_ID = "shuffler";
    private static final String RESHUFFLE_BOLT_ID = "reshuffler";
    private static final String JOINER_R_BOLT_ID = "joiner-r";
    private static final String JOINER_S_BOLT_ID = "joiner-s";
    private static final String JOINER_T_BOLT_ID = "joiner-t";///TriJoin
    private static final String POST_PROCESS_BOLT_ID = "gatherer";
    private static final String AGGREGATE_BOLT_ID = "aggregator";
    public static final String  MONITOR_R_BOLT_ID = "monitor-r";
    public static final String  MONITOR_S_BOLT_ID = "monitor-s";

    public static final String SHUFFLE_R_STREAM_ID = "shuffle-r";
    public static final String SHUFFLE_S_STREAM_ID = "shuffle-s";
    public static final String SHUFFLE_T_STREAM_ID = "shuffle-t"; ///TriJoin
    public static final String BROADCAST_R_STREAM_ID = "broadcast-r";
    public static final String BROADCAST_S_STREAM_ID = "broadcast-s";
    public static final String BROADCAST_T_STREAM_ID = "broadcast-t"; ////TriJoin
    public static final String MONITOR_R_STREAM_ID = "monitor-stream-r";
    public static final String MONITOR_S_STREAM_ID = "monitor-stream-s";
    public static final String MONITOR_R_TO_JOINER_STREAM_ID = "monitor-joiner-stream-r";
    public static final String MONITOR_S_TO_JOINER_STREAM_ID = "monitor-joiner-stream-s";
    public static final String JOINER_TO_JOINER_STREAM_ID = "joiner-joiner-stream-";
    public static final String JOINER_TO_JOINER_MODIFY_STREAM_ID = "joiner-joiner-modifyShard-stream";
    public static final String JOINER_TO_POST_STREAM_ID = "joiner-post-stream";
    public static final String JOINER_TO_MONITOR_STREAM_ID = "joiner-monitor-stream";
    public static final String JOINER_TO_RESHUFFLER_STREAM_ID = "joiner-reshuffler-stream";
    public static final String SHARD_STREAM_ID = "send-shard-stream";
    public static final String RESHUFFLER_TO_MONITOR_R_STREAM_ID = "reshuffler-monitor-stream-r";
    public static final String RESHUFFLER_TO_MONITOR_S_STREAM_ID = "reshuffler-monitor-stream-s";

    public static final String KAFKA_SPOTU_ID_R ="kafka-spout-r";
    public static final String KAFKA_SPOTU_ID_S ="kafka-spout-s";
    public static final String KAFKA_SPOTU_ID_T ="kafka-spout-t";
    //public static final String KAFKA_TEST_BOLT_ID = "kafka-test";
//    public static final String KAFKA_BROKER = "node24:9092,node25:9092,node26:9092,node27:9092,node28:9092";
//    public static final String KAFKA_BROKER = "node95:9092,node96:9092,node97:9092,node98:9092,node99:9092";
    public static final String KAFKA_BROKER = "node104:9092";



    private final TopologyArgs _args = new TopologyArgs("KafkaTopology");

    public int run(String[] args) throws Exception {
        if (!_args.processArgs(args))
            return -1;
        if (_args.help)
            return 0;
        else
            // _args.logArgs();
            writeSettingsToFile();

        /* build topology */
        StormTopology topology = createTopology();
        if (topology == null)
            return -2;
        logTopology(LOG, topology);

        /* configure topology */
        Config conf = configureTopology();
        if (conf == null)
            return -3;
        LOG.info("configuration: " + conf.toString());
        LOG.info("groupid: " + _args.groupid);
        LOG.info("topic: " + _args.topic);

        /* run topology */
        if (_args.remoteMode) {
            LOG.info("execution mode: remote");
            runInCluster(_args.topologyName, topology, conf);
        }
        else {
            LOG.info("execution mode: local");
            writeSettingsToFile();
            runLocally(_args.topologyName, topology, conf, _args.localRuntime);
        }

        return 0;
    }

    private StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOTU_ID_R, new com.basic.core.Component.RandomDataSpout("R", _args.inputfile, _args.tuplesPerSecondR), _args.numKafkaSpouts);
        builder.setSpout(KAFKA_SPOTU_ID_S, new com.basic.core.Component.RandomDataSpout("S", _args.inputfile, _args.tuplesPerSecondS), _args.numKafkaSpouts);

//        builder.setSpout(KAFKA_SPOTU_ID_R, new KafkaSpout<>(getKafkaSpoutConfig(KAFKA_BROKER, "didiOrder" + _args.dataSize, _args.groupid)), _args.numKafkaSpouts);
//        builder.setSpout(KAFKA_SPOTU_ID_S, new KafkaSpout<>(getKafkaSpoutConfig(KAFKA_BROKER, "didiGps" + _args.dataSize, _args.groupid)), _args.numKafkaSpouts);
        builder.setBolt(SHUFFLE_BOLT_ID, new ShuffleBolt(_args.dataSize), _args.numShufflers)
                .shuffleGrouping(KAFKA_SPOTU_ID_R)
                .shuffleGrouping(KAFKA_SPOTU_ID_S);

        JoinBolt joinerR = new JoinBolt("R", _args.interval, _args.window, _args.winR, _args.e1, _args.e2, _args.numShards,
                _args.conMigOrNot, _args.jumpMigOrNot, _args.penalAlpha, _args.rewardAlpha, _args.numInitShardPerTask);
        JoinBolt joinerS = new JoinBolt("S", _args.interval, _args.window, _args.winS, _args.e1, _args.e2, _args.numShards,
                _args.conMigOrNot, _args.jumpMigOrNot, _args.penalAlpha, _args.rewardAlpha, _args.numInitShardPerTask);

//        if(_args.strategy.equals(TopologyArgs.HASH_STRATEGY)){
//            builder.setBolt(RESHUFFLE_BOLT_ID,
//                    new ReshuffleBolt("hash", _args.numPartitionsR, _args.numPartitionsS, _args.interval, JOINER_R_BOLT_ID, JOINER_S_BOLT_ID, _args.e1, _args.e2),
//                    _args.numReshufflers)
//                    .shuffleGrouping(SHUFFLE_BOLT_ID)
//                    .allGrouping(JOINER_R_BOLT_ID, JOINER_TO_RESHUFFLER_STREAM_ID)
//                    .allGrouping(JOINER_S_BOLT_ID, JOINER_TO_RESHUFFLER_STREAM_ID);
//
//            Log.info("hash strategy");
//            builder.setBolt(JOINER_R_BOLT_ID, joinerR, _args.numPartitionsR)
//                    .fieldsGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_R_STREAM_ID, new Fields("key"))
//                    .fieldsGrouping(RESHUFFLE_BOLT_ID, BROADCAST_S_STREAM_ID, new Fields("key"));
//
//            builder.setBolt(JOINER_S_BOLT_ID, joinerS, _args.numPartitionsS)
//                    .fieldsGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_S_STREAM_ID, new Fields("key"))
//                    .fieldsGrouping(RESHUFFLE_BOLT_ID, BROADCAST_R_STREAM_ID, new Fields("key"));
//
//            MonitorBolt monitorBoltR = new MonitorBolt("R", _args.numPartitionsR + _args.numPartitionsS, _args.window, _args.winInSecR, _args.startTime, _args.threshold, _args.interval, _args.numReshufflers);
//            MonitorBolt monitorBoltS = new MonitorBolt("S", _args.numPartitionsR + _args.numPartitionsS, _args.window, _args.winInSecS, _args.startTime, _args.threshold, _args.interval, _args.numReshufflers);
//
//            builder.setBolt(MONITOR_R_BOLT_ID, monitorBoltR, 1)
//                    .globalGrouping(RESHUFFLE_BOLT_ID, MONITOR_R_STREAM_ID);
//                    //.globalGrouping(JOINER_R_BOLT_ID, JOINER_TO_MONITOR_STREAM_ID);
//
//            builder.setBolt(MONITOR_S_BOLT_ID, monitorBoltS, 1)
//                    .globalGrouping(RESHUFFLE_BOLT_ID, MONITOR_S_STREAM_ID);
//                    //.globalGrouping(JOINER_S_BOLT_ID, JOINER_TO_MONITOR_STREAM_ID);
//        }
//        else if(_args.strategy.equals(TopologyArgs.AVE_STRATEGY)){
//            builder.setBolt(RESHUFFLE_BOLT_ID,
//                    new ReshuffleBolt("average", _args.numPartitionsR, _args.numPartitionsS, _args.interval, JOINER_R_BOLT_ID, JOINER_S_BOLT_ID, _args.e1, _args.e2),
//                    _args.numReshufflers)
//                    .shuffleGrouping(SHUFFLE_BOLT_ID);/////////////numinstance应该是joinerR和joinerS的数量。
////                    .allGrouping(JOINER_R_BOLT_ID, JOINER_TO_RESHUFFLER_STREAM_ID)
////                    .allGrouping(JOINER_S_BOLT_ID, JOINER_TO_RESHUFFLER_STREAM_ID);
//
//            Log.info("average strategy");
//            builder.setBolt(JOINER_R_BOLT_ID, joinerR, _args.numPartitionsR)
//                    .customGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_R_STREAM_ID, new FastJoinGrouping())
//                    .customGrouping(RESHUFFLE_BOLT_ID, BROADCAST_S_STREAM_ID, new FastJoinGrouping());
//
//            builder.setBolt(JOINER_S_BOLT_ID, joinerS, _args.numPartitionsS)
//                    .customGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_S_STREAM_ID, new FastJoinGrouping())
//                    .customGrouping(RESHUFFLE_BOLT_ID, BROADCAST_R_STREAM_ID, new FastJoinGrouping());
//
//            MonitorBolt monitorBoltR = new MonitorBolt("R", _args.numPartitionsR, _args.window, _args.winInSecR, _args.startTime, _args.threshold, _args.interval, _args.numReshufflers);
//            MonitorBolt monitorBoltS = new MonitorBolt("S", _args.numPartitionsS, _args.window, _args.winInSecS, _args.startTime, _args.threshold, _args.interval, _args.numReshufflers);
//
//            builder.setBolt(MONITOR_R_BOLT_ID, monitorBoltR, 1)
//                    .globalGrouping(JOINER_R_BOLT_ID, MONITOR_R_STREAM_ID)
//                    .globalGrouping(RESHUFFLE_BOLT_ID, MONITOR_R_STREAM_ID);
//
//            builder.setBolt(MONITOR_S_BOLT_ID, monitorBoltS, 1)
//                    .globalGrouping(JOINER_S_BOLT_ID, MONITOR_S_STREAM_ID)
//                    .globalGrouping(RESHUFFLE_BOLT_ID, MONITOR_S_STREAM_ID);
//        }
//        else
            if(_args.strategy.equals(TopologyArgs.SA_STRATEGY)){
            Log.info("sajoin strategy");
            builder.setBolt(RESHUFFLE_BOLT_ID,
                    new ReshuffleBolt("sajoin", _args.numPartitionsR, _args.numPartitionsS, _args.interval, JOINER_R_BOLT_ID, JOINER_S_BOLT_ID, _args.e1, _args.e2, _args.numInitShardPerTask),
                    _args.numReshufflers)
                    .shuffleGrouping(SHUFFLE_BOLT_ID)
                    .allGrouping(JOINER_R_BOLT_ID, JOINER_TO_RESHUFFLER_STREAM_ID)
                    .allGrouping(JOINER_S_BOLT_ID, JOINER_TO_RESHUFFLER_STREAM_ID);

            builder.setBolt(JOINER_R_BOLT_ID, joinerR, _args.numPartitionsR)
                    .customGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_R_STREAM_ID, new BandJoinGrouping())
                    .customGrouping(RESHUFFLE_BOLT_ID, BROADCAST_S_STREAM_ID, new BandJoinGrouping())
                    .directGrouping(MONITOR_R_BOLT_ID, MONITOR_R_TO_JOINER_STREAM_ID)
                    .directGrouping(JOINER_R_BOLT_ID, JOINER_TO_JOINER_STREAM_ID)
                    .directGrouping(JOINER_R_BOLT_ID, JOINER_TO_JOINER_MODIFY_STREAM_ID);

            builder.setBolt(JOINER_S_BOLT_ID, joinerS, _args.numPartitionsS)
                    .customGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_S_STREAM_ID, new BandJoinGrouping())
                    .customGrouping(RESHUFFLE_BOLT_ID, BROADCAST_R_STREAM_ID, new BandJoinGrouping())
                    .directGrouping(MONITOR_S_BOLT_ID, MONITOR_S_TO_JOINER_STREAM_ID)
                    .directGrouping(JOINER_S_BOLT_ID, JOINER_TO_JOINER_STREAM_ID)
                    .directGrouping(JOINER_S_BOLT_ID, JOINER_TO_JOINER_MODIFY_STREAM_ID);
            ////MonitorBolt收到的shard全启动的barrier数量暂定numPartition。
            MonitorBolt monitorBoltR = new MonitorBolt("R", _args.numPartitionsR, _args.window, _args.winInSecR, _args.startTime,
                    _args.threshold, true, _args.interval, _args.numReshufflers, _args.numInitShardPerTask, _args.e1, _args.e2);
            MonitorBolt monitorBoltS = new MonitorBolt("S", _args.numPartitionsS, _args.window, _args.winInSecS, _args.startTime,
                    _args.threshold, true, _args.interval, _args.numReshufflers, _args.numInitShardPerTask, _args.e1, _args.e2);

            builder.setBolt(MONITOR_R_BOLT_ID, monitorBoltR, 1)
                    .globalGrouping(JOINER_R_BOLT_ID, MONITOR_R_STREAM_ID)
                    .globalGrouping(RESHUFFLE_BOLT_ID, RESHUFFLER_TO_MONITOR_R_STREAM_ID);

            builder.setBolt(MONITOR_S_BOLT_ID, monitorBoltS, 1)
                    .globalGrouping(JOINER_S_BOLT_ID, MONITOR_S_STREAM_ID)
                    .globalGrouping(RESHUFFLE_BOLT_ID, RESHUFFLER_TO_MONITOR_S_STREAM_ID);
        }
//        else if(_args.strategy.equals(TopologyArgs.RANDOM_STRATEGY)){
//            builder.setBolt(RESHUFFLE_BOLT_ID,
//                    new ReshuffleBolt("random", _args.numPartitionsR, _args.numPartitionsS, _args.interval, JOINER_R_BOLT_ID, JOINER_S_BOLT_ID, _args.e1, _args.e2),
//                    _args.numReshufflers)
//                    .shuffleGrouping(SHUFFLE_BOLT_ID)
//                    .allGrouping(JOINER_R_BOLT_ID, JOINER_TO_RESHUFFLER_STREAM_ID)
//                    .allGrouping(JOINER_S_BOLT_ID, JOINER_TO_RESHUFFLER_STREAM_ID);
//
//            Log.info("random strategy");
//            builder.setBolt(JOINER_R_BOLT_ID, joinerR, _args.numPartitionsR)
//                    .shuffleGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_R_STREAM_ID)
//                    .allGrouping(RESHUFFLE_BOLT_ID, BROADCAST_S_STREAM_ID);
//
//
//            builder.setBolt(JOINER_S_BOLT_ID, joinerS, _args.numPartitionsS)
//                    .shuffleGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_S_STREAM_ID)
//                    .allGrouping(RESHUFFLE_BOLT_ID, BROADCAST_R_STREAM_ID);
//        }
//        else if(_args.strategy.equals(TopologyArgs.FAST_JOIN_STRATEGY)){
//            builder.setBolt(RESHUFFLE_BOLT_ID,
//                    new ReshuffleBolt("fastjoin", _args.numPartitionsR, _args.numPartitionsS, _args.interval, JOINER_R_BOLT_ID, JOINER_S_BOLT_ID, _args.e1, _args.e2),
//                    _args.numReshufflers)
//                    .shuffleGrouping(SHUFFLE_BOLT_ID)
//                    .allGrouping(JOINER_R_BOLT_ID, JOINER_TO_RESHUFFLER_STREAM_ID)
//                    .allGrouping(JOINER_S_BOLT_ID, JOINER_TO_RESHUFFLER_STREAM_ID);
//
//            //Log.info("fast join strategy");
//            // setting monitor
//            MonitorBolt monitorBoltR = new MonitorBolt("R", _args.numPartitionsR + _args.numPartitionsS, _args.window, _args.winInSecR, _args.startTime, _args.threshold, _args.interval, _args.numReshufflers);
//            MonitorBolt monitorBoltS = new MonitorBolt("S", _args.numPartitionsR + _args.numPartitionsS, _args.window, _args.winInSecS, _args.startTime, _args.threshold, _args.interval, _args.numReshufflers);
//
//
//            builder.setBolt(MONITOR_R_BOLT_ID, monitorBoltR, 1)
//                    .globalGrouping(RESHUFFLE_BOLT_ID, MONITOR_R_STREAM_ID)
//                    .globalGrouping(JOINER_R_BOLT_ID, JOINER_TO_MONITOR_STREAM_ID);
//
//            builder.setBolt(MONITOR_S_BOLT_ID, monitorBoltS, 1)
//                    .globalGrouping(RESHUFFLE_BOLT_ID, MONITOR_S_STREAM_ID)
//                    .globalGrouping(JOINER_S_BOLT_ID, JOINER_TO_MONITOR_STREAM_ID);
//
//            builder.setBolt(JOINER_R_BOLT_ID, joinerR, _args.numPartitionsR)
//                    .customGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_R_STREAM_ID, new FastJoinGrouping())
//                    .customGrouping(RESHUFFLE_BOLT_ID, BROADCAST_S_STREAM_ID, new FastJoinGrouping())
//                    //monitor 往joiner 发数据
//                    .directGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_R_STREAM_ID)
//                    //joiner 互发数据
//                    .directGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_S_STREAM_ID);
//
//            builder.setBolt(JOINER_S_BOLT_ID, joinerS, _args.numPartitionsS)
//                    .customGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_S_STREAM_ID, new FastJoinGrouping())
//                    .customGrouping(RESHUFFLE_BOLT_ID, BROADCAST_R_STREAM_ID, new FastJoinGrouping())
//                    //monitor 往joiner 发数据
//                    .directGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_R_STREAM_ID)
//                    //joiner 互发数据
//                    .directGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_S_STREAM_ID);
//
//        }
//        else if(_args.strategy.equals(TopologyArgs.CONTRAND_STRATEGY)) {
//            builder.setBolt(RESHUFFLE_BOLT_ID,
//                    new ReshuffleBolt("contrand", _args.numPartitionsR, _args.numPartitionsS, _args.interval, JOINER_R_BOLT_ID, JOINER_S_BOLT_ID, _args.e1, _args.e2),
//                    _args.numReshufflers)
//                    .shuffleGrouping(SHUFFLE_BOLT_ID)
//                    .allGrouping(JOINER_R_BOLT_ID, JOINER_TO_RESHUFFLER_STREAM_ID)
//                    .allGrouping(JOINER_S_BOLT_ID, JOINER_TO_RESHUFFLER_STREAM_ID);
//
//            Log.info("contrand strategy");
//            builder.setBolt(JOINER_R_BOLT_ID, joinerR, _args.numPartitionsR)
//                    .customGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_R_STREAM_ID, new ContRandGrouping(_args.rMembers, "R"))
//                    .customGrouping(RESHUFFLE_BOLT_ID, BROADCAST_S_STREAM_ID, new ContRandGrouping(_args.rMembers, "R"));
//
//            builder.setBolt(JOINER_S_BOLT_ID, joinerS, _args.numPartitionsS)
//                    .customGrouping(RESHUFFLE_BOLT_ID, SHUFFLE_S_STREAM_ID, new ContRandGrouping(_args.sMembers, "S"))
//                    .customGrouping(RESHUFFLE_BOLT_ID, BROADCAST_R_STREAM_ID, new ContRandGrouping(_args.sMembers, "S"));
//
//            MonitorBolt monitorBoltR = new MonitorBolt("R", _args.numPartitionsR + _args.numPartitionsS, _args.window, _args.winInSecR, _args.startTime, _args.threshold, _args.interval, _args.numReshufflers);
//            MonitorBolt monitorBoltS = new MonitorBolt("S", _args.numPartitionsR + _args.numPartitionsS, _args.window, _args.winInSecS, _args.startTime, _args.threshold, _args.interval, _args.numReshufflers);
//
//
//            builder.setBolt(MONITOR_R_BOLT_ID, monitorBoltR, 1)
//                    .globalGrouping(RESHUFFLE_BOLT_ID, MONITOR_R_STREAM_ID);
//                    //.globalGrouping(JOINER_R_BOLT_ID, JOINER_TO_MONITOR_STREAM_ID);
//
//            builder.setBolt(MONITOR_S_BOLT_ID, monitorBoltS, 1)
//                    .globalGrouping(RESHUFFLE_BOLT_ID, MONITOR_S_STREAM_ID);
//                    //.globalGrouping(JOINER_S_BOLT_ID, JOINER_TO_MONITOR_STREAM_ID);
//        }

//        builder.setBolt(POST_PROCESS_BOLT_ID, new PostProcessBolt(_args.numReshufflers), 1)
//                .globalGrouping(RESHUFFLE_BOLT_ID, JOINER_TO_POST_STREAM_ID);

        builder.setBolt(POST_PROCESS_BOLT_ID, new PostProcessBolt( _args.numPartitionsS + _args.numPartitionsR), 1)
            .globalGrouping(JOINER_R_BOLT_ID, JOINER_TO_POST_STREAM_ID)
            .globalGrouping(JOINER_S_BOLT_ID, JOINER_TO_POST_STREAM_ID);
        return builder.createTopology();
    }

    private Config configureTopology() {
        Config conf = new Config();
        _args.topologyName += "-" + _args.strategy;
        conf.setDebug(_args.debug);
        conf.setNumWorkers(_args.numWorkers);
        conf.setNumAckers(_args.numShufflers);
        conf.put("joinFieldIdxR", _args.joinFieldIdxR);
        conf.put("joinFieldIdxS", _args.joinFieldIdxS);
        conf.put("operator", _args.operator);

//        if (_args.numGenerators > 0) {
//            conf.put("tuplesPerSecond", _args.tuplesPerSecond);
//        }
//        if (_args.numGeneratorsR > 0) {
//            conf.put("tuplesPerSecondR", _args.tuplesPerSecondR);
//        }
//        if (_args.numGeneratorsS > 0) {
//            conf.put("tuplesPerSecondS", _args.tuplesPerSecondS);
//        }
        conf.put("fluctuation", _args.fluctuation);

        conf.put("subindexSize", _args.subindexSize);

        conf.put("window", _args.window);
        conf.put("winR", _args.winR);
        conf.put("winS", _args.winS);

        conf.put("dedup", !_args.noDedup);
        conf.put("dedupSize", _args.dedupSize);

        conf.put("aggregate", _args.aggregate);
        conf.put("aggReportInSeconds", _args.aggReportInSeconds);

        conf.put("noOutput", _args.noOutput);
        conf.put("outputDir", _args.outputDir);
        conf.put("simple", _args.simple);

        conf.put("intLower", _args.intLower);
        conf.put("intUpper", _args.intUpper);
        conf.put("doubleLower", _args.doubleLower);
        conf.put("doubleUpper", _args.doubleUpper);
        conf.put("charsLength", _args.charsLength);

        return conf;
    }

    private void writeSettingsToFile() {
        FileWriter output = new FileWriter(_args.outputDir, "top", "txt")
                .setPrintStream(System.out);
        _args.logArgs(output);
        output.endOfFile();
    }

    public static void main(String[] args) throws Exception {
        int rc = (new KafkaTopology()).run(args);
        LOG.info("return code: " + rc);
    }


    public static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers,String topic, String groupid) {
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
                new Fields("topic", "partition", "offset", "key", "value"));
        return KafkaSpoutConfig.builder(bootstrapServers, new String[]{topic})
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, groupid)
                .setRetry(getRetryService())
                .setRecordTranslator(trans)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
                .build();
    }
    public static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(0),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }
}