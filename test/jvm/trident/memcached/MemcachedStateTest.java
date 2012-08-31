package trident.memcached;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;

import com.twitter.finagle.memcached.java.Client;

import junit.framework.TestCase;
import storm.trident.state.Serializer;

public class MemcachedStateTest extends TestCase {
  private static final MemCacheDaemon<LocalCacheElement> daemon =
      new MemCacheDaemon<LocalCacheElement>();
  private static final int PORT = 11211;
  private static final long SEED = 8682522807148012L;
  private static final Random RANDOM = new Random(SEED);

  public void testCache() throws Exception {
    // Start memcached.
    CacheStorage<Key, LocalCacheElement> storage =
        ConcurrentLinkedHashMap.create(
                                          ConcurrentLinkedHashMap.EvictionPolicy.FIFO, 100, 1024 * 500);
    daemon.setCache(new CacheImpl(storage));
    daemon.setAddr(new InetSocketAddress("localhost", PORT));
    daemon.start();

    // Build the cache client.
    Client client = MemcachedState.Factory.makeMemcachedClient(Arrays.asList(new InetSocketAddress("localhost", PORT)));
    MemcachedState<Integer> state = new MemcachedState<Integer>(client, new Serializer<Integer>() {
      @Override
      public byte[] serialize(Integer integer) {
        return Ints.toByteArray(integer);
      }

      @Override
      public Object deserialize(byte[] bytes) {
        return Ints.fromByteArray(bytes);
      }
    });

    // Insert some kv pairs.
    String[] keys = new String[]{"foo", "bar", "baz"};
    List<Integer> vals = Lists.newArrayList();
    List<List<Object>> keyList = Lists.newArrayList();
    for (String key : keys) {
      List<Object> l = Lists.newArrayList();
      l.add(key);
      keyList.add(l);
      vals.add(RANDOM.nextInt());
    }
    state.multiPut(keyList, vals);

    // Verify if the retrieval of the kv pairs.
    List<Integer> actualVals = state.multiGet(keyList);
    assertEquals(vals.size(), actualVals.size());
    for (int i = 0; i < vals.size(); i++) {
      assertEquals(vals.get(i), actualVals.get(i));
    }
  }
}
