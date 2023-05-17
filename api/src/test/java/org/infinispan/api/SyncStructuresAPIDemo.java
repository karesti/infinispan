package org.infinispan.api;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientAtomicLong;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientCompute;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.sync.SyncCache;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncList;
import org.infinispan.api.sync.SyncMultimap;
import org.infinispan.api.sync.SyncSet;
import org.infinispan.api.sync.SyncSortedSet;

import java.util.concurrent.TimeUnit;

/**
 * @author Katia Aresti
 * @since 15.0
 **/
public class SyncStructuresAPIDemo {

   public static void main(String[] args) {
     hazelcast();
   }

   public static void redis() {
      RedisClient client = RedisClient.create("redis://HOST:PORT");
      StatefulRedisConnection<String, String> connection = client.connect();
      RedisCommands<String, String> sync = connection.sync();

      // list
      sync.lpush("list", "value1", "value2", "value3");

      // set
      sync.sadd("set", "value1", "value2");

      //sorted set
      sync.zadd("sortedSet", 1, "value1", 2, "value2");
   }

   /**
    * One single dependency
    *
    *       <dependency>
    *            <groupId>org.apache.ignite</groupId>
    *            <artifactId>ignite-core</artifactId>
    *            <version>2.15.0</version>
    *        </dependency>
    *
    * @throws InterruptedException
    */
   public static void ignite() throws InterruptedException {
      ClientConfiguration cfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
      try (IgniteClient client = Ignition.startClient(cfg)) {

         ClientCache<Integer, String> cache = client.cache("myCache");
         cache.put(1, "value1");
         cache.putAsync(2, "value2");

         ClientAtomicLong clientAtomicLong = client.atomicLong("counter", 12, true);
         clientAtomicLong.addAndGet(12);

         client.createCache(new ClientCacheConfiguration());
         client.createCacheAsync(new ClientCacheConfiguration());
         client.query(new SqlFieldsQuery("select from People", true));

         IgniteBinary binary = client.binary();
         binary.buildEnum("string", 1);

         ClientCompute compute = client.compute();
         compute.execute("task", 12);

         client.transactions().txStart();
         //...
      }

   }

   /**
    * One single dependency
    *
    *        <dependency>
    *            <groupId>com.hazelcast</groupId>
    *            <artifactId>hazelcast</artifactId>
    *            <version>5.3.0</version>
    *        </dependency>
    */
   public static void hazelcast() {
      // Replace 1 line for embedded mode: Hazelcast.newHazelcastInstance();
      // Unsupported operations exceptions in runtime
      HazelcastInstance hz = HazelcastClient.newHazelcastClient();
      System.out.println(hz.getName());

      IMap<String, String> myMap = hz.getMap("myMap");
      myMap.put("key", "value", 122, TimeUnit.MINUTES);
      System.out.println(myMap.getName());
      System.out.println(myMap.get("key"));

      MultiMap<String, String> multimap = hz.getMultiMap("multimap");
      multimap.put("key", "value");
      multimap.put("key", "value1");
      multimap.put("key", "valu2");
      System.out.println(multimap.getName());
      System.out.println(multimap.get("key"));

      IList<String> mylist = hz.getList("mylist");
      mylist.add(0, "value1");
      mylist.add(1, "value1");
      mylist.add(2, "value1");

      System.out.println(mylist.getName());
      System.out.println(mylist.size());

      ISet<String> mySet = hz.getSet("mySet");
      mySet.add("value2");
      mySet.add("value1");
      System.out.println(mySet.getName());
      System.out.println(mySet.size());
   }


   public void infinispan() {
      try (SyncContainer infinispan = Infinispan.create("file:///path/to/infinispan.xml").sync()) {
         SyncCache<String, String> cache = infinispan.sync().getCache("cache1");
         cache.put("key", "value", CacheWriteOptions.DEFAULT);

         SyncList<String> list = infinispan.sync().getList("listName1");
         SyncSortedSet<String> sortedSet = infinispan.sync().getSortedSet("sortedSet1");
         SyncSet set = infinispan.sync().getSet("set1");
         SyncMultimap<String, String> multimap = infinispan.sync().getMultimap("multimap");
         list.offerFirst("value");
         sortedSet.add("value");
         set.add("value");
         multimap.add("key", "value");

         // additional structure container
         SyncList<String> list2 = infinispan.sync().structures().list("name1", "listName2");
         SyncSet<String> set2 = infinispan.sync().structures().set("name2", "setName2");
         SyncSortedSet<String> sortedSet2 = infinispan.sync().structures().sortedSet("name3", "sortedSet3");
         list2.offerFirst("value");
         set2.add("value");
         sortedSet2.add("value");
      }
   }
}
