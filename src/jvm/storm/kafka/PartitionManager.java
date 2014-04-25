package storm.kafka;

import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;
import com.google.common.collect.ImmutableMap;
import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout.EmitState;
import storm.kafka.KafkaSpout.MessageAndRealOffset;
import storm.kafka.KafkaUtils.Response;
import storm.kafka.trident.MaxMetric;

import java.util.*;

public class PartitionManager {
  public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);
  private final CombinedMetric _fetchAPILatencyMax;
  private final ReducedMetric _fetchAPILatencyMean;
  private final CountMetric _fetchAPICallCount;
  private final CountMetric _fetchAPIMessageCount;

  static class KafkaMessageId {
    public Partition partition;
    public long offset;

    public KafkaMessageId(Partition partition, long offset) {
      this.partition = partition;
      this.offset = offset;
    }
  }

  Long _emittedToOffset;
  SortedSet<Long> _pending = new TreeSet<Long>();
  SortedSet<Long> failed = new TreeSet<Long>();
  Long _committedTo;
  LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
  Partition _partition;
  SpoutConfig _spoutConfig;
  String _topologyInstanceId;
  SimpleConsumer _consumer;
  DynamicPartitionConnections _connections;
  ZkState _state;
  Map _stormConf;
  long numberFailed, numberAcked;


  public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId, ZkState state, Map stormConf, SpoutConfig spoutConfig, Partition id) {
    _partition = id;
    _connections = connections;
    _spoutConfig = spoutConfig;
    _topologyInstanceId = topologyInstanceId;
    _consumer = connections.register(id.host, id.partition);
    _state = state;
    _stormConf = stormConf;
    numberAcked = numberFailed = 0;

    String jsonTopologyId = null;
    Long jsonOffset = null;
    String path = committedPath();
    try {
      Map<Object, Object> json = _state.readJSON(path);
      LOG.info("Read partition information from: " + path +  "  --> " + json );
      if (json != null) {
        jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
        jsonOffset = (Long) json.get("offset");
      }
    } catch (Throwable e) {
      LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
    }

    Long currentOffset = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig);

    if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
      _committedTo = currentOffset;
      LOG.info("No partition information found, using configuration to determine offset");
    } else if (!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.forceFromStart) {
      _committedTo = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig.startOffsetTime);
      LOG.info("Topology change detected and reset from start forced, using configuration to determine offset");
    } else {
      _committedTo = jsonOffset;
      LOG.info("Read last commit offset from zookeeper: " + _committedTo + "; old topology_id: " + jsonTopologyId + " - new topology_id: " + topologyInstanceId );
    }

    if (currentOffset - _committedTo > spoutConfig.maxOffsetBehind || _committedTo <= 0) {
      LOG.info("Last commit offset from zookeeper: " + _committedTo);
      _committedTo = currentOffset;
      LOG.info("Commit offset " + _committedTo + " is more than " +
          spoutConfig.maxOffsetBehind + " behind, resetting to HEAD.");
    }

    LOG.info("Starting Kafka " + _consumer.host() + ":" + id.partition + " from offset " + _committedTo);
    _emittedToOffset = _committedTo;

    _fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
    _fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
    _fetchAPICallCount = new CountMetric();
    _fetchAPIMessageCount = new CountMetric();
  }

  public Map getMetricsDataMap() {
    Map ret = new HashMap();
    ret.put(_partition + "/fetchAPILatencyMax", _fetchAPILatencyMax.getValueAndReset());
    ret.put(_partition + "/fetchAPILatencyMean", _fetchAPILatencyMean.getValueAndReset());
    ret.put(_partition + "/fetchAPICallCount", _fetchAPICallCount.getValueAndReset());
    ret.put(_partition + "/fetchAPIMessageCount", _fetchAPIMessageCount.getValueAndReset());
    return ret;
  }

  //returns false if it's reached the end of current batch
  public EmitState next(SpoutOutputCollector collector) {
    if (_waitingToEmit.isEmpty()) {
      fill();
    }
    while (true) {
      MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
      if (toEmit == null) {
        return EmitState.NO_EMITTED;
      }
      Iterable<List<Object>> tups = _spoutConfig.scheme.deserialize(Utils.toByteArray(toEmit.msg.payload()));
      if (tups != null) {
        for (List<Object> tup : tups) {
          collector.emit(tup, new KafkaMessageId(_partition, toEmit.offset));
        }
        break;
      } else {
        ack(toEmit.offset);
      }
    }
    if (!_waitingToEmit.isEmpty()) {
      return EmitState.EMITTED_MORE_LEFT;
    } else {
      return EmitState.EMITTED_END;
    }
  }

  private void fill() {
    long start = System.nanoTime();
    long offset;
    final boolean had_failed = !failed.isEmpty();

    // Are there failed tuples? If so, fetch those first.
    if (had_failed) {
      offset = failed.first();
    } else {
      offset = _emittedToOffset + 1;
    }

    Response response = KafkaUtils.fetchMessages(_spoutConfig, _consumer, _partition, offset);
    long end = System.nanoTime();
    long millis = (end - start) / 1000000;
    _fetchAPILatencyMax.update(millis);
    _fetchAPILatencyMean.update(millis);
    _fetchAPICallCount.incr();
    if (response.msgs != null) {
      int numMessages = 0;

      for (MessageAndOffset msg : response.msgs) {
        final Long cur_offset = msg.offset();
        if (!had_failed || failed.contains(cur_offset)) {
          numMessages += 1;
          _pending.add(cur_offset);
          _waitingToEmit.add(new MessageAndRealOffset(msg.message(), cur_offset));
          _emittedToOffset = Math.max(cur_offset, _emittedToOffset);
          if (had_failed) {
            failed.remove(cur_offset);
          }
        }
      }
      _fetchAPIMessageCount.incrBy(numMessages);
    }
  }

  public void ack(Long offset) {
    if (!_pending.isEmpty() && _pending.first() < offset - _spoutConfig.maxOffsetBehind) {
      // Too many things pending!
      _pending.headSet(offset).clear();
    } else {
      _pending.remove(offset);
    }
    numberAcked++;
  }

  public void fail(Long offset) {
    if (offset < _emittedToOffset - _spoutConfig.maxOffsetBehind) {
      LOG.info(
          "Skipping failed tuple at offset=" + offset +
          " because it's more than maxOffsetBehind=" + _spoutConfig.maxOffsetBehind +
          " behind _emittedToOffset=" + _emittedToOffset
      );
    } else {
      LOG.debug("failing at offset=" + offset + " with _pending.size()=" + _pending.size() + " pending and _emittedToOffset=" + _emittedToOffset);
      failed.add(offset);
      numberFailed++;
      if (numberAcked == 0 && numberFailed > _spoutConfig.maxOffsetBehind) {
        throw new RuntimeException("Too many tuple failures");
      }
    }
  }

  public void commit() {
    LOG.debug("Committing offset for " + _partition);
    long committedTo;
    if (_pending.isEmpty()) {
      committedTo = _emittedToOffset;
    } else {
      committedTo = _pending.first() - 1;
    }
    if (committedTo != _committedTo) {
      LOG.debug("Writing committed offset to ZK: " + committedTo);

      Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
          .put("topology", ImmutableMap.of("id", _topologyInstanceId,
              "name", _stormConf.get(Config.TOPOLOGY_NAME)))
          .put("offset", committedTo)
          .put("partition", _partition.partition)
          .put("broker", ImmutableMap.of("host", _partition.host.host,
              "port", _partition.host.port))
          .put("topic", _spoutConfig.topic).build();
      _state.writeJSON(committedPath(), data);

      //LOG.info("Wrote committed offset to ZK: " + committedTo);
      _committedTo = committedTo;
    }
    LOG.debug("Committed offset " + committedTo + " for " + _partition + " for topology: " + _topologyInstanceId);
  }

  private String committedPath() {
    return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition.getId();
  }

  public long lastCompletedOffset() {
    if (_pending.isEmpty()) {
      return _emittedToOffset;
    } else {
      return _pending.first();
    }
  }

  public Partition getPartition() {
    return _partition;
  }

  public void close() {
    _connections.unregister(_partition.host, _partition.partition);
  }
}
