package trident.memcached;

import backtype.storm.tuple.Values;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.memcached.KetamaClientBuilder;
import com.twitter.finagle.memcached.java.Client;
import com.twitter.finagle.memcached.java.ClientBase;
import com.twitter.finagle.memcached.protocol.text.Memcached;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.Time;

import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

public class MemcachedState<T> implements IBackingMap<T> {
    private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = new HashMap<StateType, Serializer>() {{
        put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }};

    public static class Options<T> implements Serializable {
        int localCacheSize = 1000;
        String globalKey = "$GLOBAL$";
        Serializer<T> serializer = null;
    }

    public static StateFactory opaque(List<InetSocketAddress> servers) {
        return opaque(servers, new Options());
    }

    public static StateFactory opaque(List<InetSocketAddress> servers, Options<OpaqueValue> opts) {
        return new Factory(servers, StateType.OPAQUE, opts);
    }

    public static StateFactory transactional(List<InetSocketAddress> servers) {
        return transactional(servers, new Options());
    }

    public static StateFactory transactional(List<InetSocketAddress> servers, Options<TransactionalValue> opts) {
        return new Factory(servers, StateType.TRANSACTIONAL, opts);
    }

    public static StateFactory nonTransactional(List<InetSocketAddress> servers) {
        return nonTransactional(servers, new Options());
    }

    public static StateFactory nonTransactional(List<InetSocketAddress> servers, Options<Object> opts) {
        return new Factory(servers, StateType.NON_TRANSACTIONAL, opts);
    }

    protected static class Factory implements StateFactory {
        StateType _type;
        List<InetSocketAddress> _servers;
        Serializer _ser;
        Options _opts;

        public Factory(List<InetSocketAddress> servers, StateType type, Options options) {
            _type = type;
            _servers = servers;
            _opts = options;
            if(options.serializer==null) {
                _ser = DEFAULT_SERIALZERS.get(type);
                if(_ser==null) {
                    throw new RuntimeException("Couldn't find serializer for state type: " + type);
                }
            } else {
                _ser = options.serializer;
            }
        }

        @Override
        public State makeState(Map conf, int partitionIndex, int numPartitions) {
            MemcachedState s;
            try {
                s = new MemcachedState(makeMemcachedClient(_servers), _ser);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
            CachedMap c = new CachedMap(s, _opts.localCacheSize);
            MapState ms;
            if(_type == StateType.NON_TRANSACTIONAL) {
                ms = NonTransactionalMap.build(c);
            } else if(_type==StateType.OPAQUE) {
                ms = OpaqueMap.build(c);
            } else if(_type==StateType.TRANSACTIONAL){
                ms = TransactionalMap.build(c);
            } else {
                throw new RuntimeException("Unknown state type: " + _type);
            }
            return new SnapshottableMap(ms, new Values(_opts.globalKey));
        }

      /**
       * Constructs a finagle java memcached client for the list of endpoints..
       *
       * @param endpoints list of {@code InetSocketAddress} for all the memcached servers.
       * @return {@link Client} to read/write to the hash ring of the servers..
       */
      static Client makeMemcachedClient(List<InetSocketAddress> endpoints)
          throws UnknownHostException {
        final int requestRetries = 2;         // max number of retries after the first failure.
        final int connectTimeoutMillis = 200; // tcp connection timeout.
        final int requestTimeoutMillis = 50;  // request timeout.
        final int e2eTimeoutMillis = 500;     // end-to-end request timeout.
        final int hostConnectionLimit = 10;   // concurrent connections to one server.
        final int maxWaiters = 2;             // max waiters in the request queue.

        com.twitter.finagle.memcached.Client client =
            KetamaClientBuilder.get()
                .nodes(getHostPortWeightTuples(endpoints))
                .clientBuilder(ClientBuilder.get()
                                   .codec(new Memcached())
                                   .tcpConnectTimeout(new Duration(TimeUnit.MILLISECONDS.toNanos(connectTimeoutMillis)))
                                   .requestTimeout(new Duration(TimeUnit.MILLISECONDS.toNanos(requestTimeoutMillis)))
                                   .timeout(new Duration(TimeUnit.MILLISECONDS.toNanos(e2eTimeoutMillis)))
                                   .hostConnectionLimit(hostConnectionLimit)
                                   .hostConnectionMaxWaiters(maxWaiters)
                                   .retries(requestRetries))
                .build();

        return new ClientBase(client);
      }

      /**
       * Constructs a host:port:weight tuples string of all the passed endpoints.
       *
       * @param endpoints list of {@code InetSocketAddress} for all the memcached servers.
       * @return Comma-separated string of host:port:weight tuples.
       */
      static String getHostPortWeightTuples(List<InetSocketAddress> endpoints) throws UnknownHostException {
          final int defaultWeight = 1;
          final StringBuilder tuples = new StringBuilder(1024);
          for (InetSocketAddress endpoint : endpoints) {
              if (tuples.length() > 0) {
                  tuples.append(",");
              }
              tuples.append(String.format("%s:%d:%d", endpoint.getHostName(), endpoint.getPort(), defaultWeight));
          }
          return tuples.toString();
      }
    }

    private final Client _client;
    private final Serializer<T> _serializer;

    public MemcachedState(Client client, Serializer<T> serializer) {
        _client = Preconditions.checkNotNull(client);
        _serializer = Preconditions.checkNotNull(serializer);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<String> singleKeys = new ArrayList();
        for(List<Object> key: keys) {
            singleKeys.add(toSingleKey(key));
        }
        Map<String, ChannelBuffer> result = _client.get(singleKeys).get();
        List<T> ret = new ArrayList(singleKeys.size());
        for(String k: singleKeys) {
            ChannelBuffer entry = result.get(k);
            if (entry != null) {
              T val = (T)_serializer.deserialize(entry.array());
              ret.add(val);
            } else {
              ret.add(null);
            }
        }
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
      final long defaultExpirtyMillis = 7 * 24 * 60 * 60 * 1000; // 7 days.
      List<Future> futures = new ArrayList(keys.size());
        for(int i=0; i<keys.size(); i++) {
            String key = toSingleKey(keys.get(i));
            T val = vals.get(i);
            byte[] serialized = _serializer.serialize(val);
            final ChannelBuffer entry = ChannelBuffers.wrappedBuffer(serialized);
            Time expiry =
                  Time.fromMilliseconds(Time.now().inMilliseconds() + defaultExpirtyMillis);
            futures.add(_client.set(key, 0 /* no flags */, expiry, entry));
        }

      //TODO: Do we need to block on the success of put ?
      for(Future future: futures) {
          future.get();
      }
    }

    private String toSingleKey(List<Object> key) {
        if(key.size()!=1) {
            throw new RuntimeException("Memcached state does not support compound keys");
        }
      //TODO: Can we return a smaller key here, typically base64 hash of the key is preferable.
      return (String) key.get(0);
    }
}
