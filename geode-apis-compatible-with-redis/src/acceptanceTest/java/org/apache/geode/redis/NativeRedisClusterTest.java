package org.apache.geode.redis;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisMovedDataException;

import org.apache.geode.NativeRedisClusterTestRule;

public class NativeRedisClusterTest {

  @ClassRule
  public static NativeRedisClusterTestRule cluster = new NativeRedisClusterTestRule();

  @Test
  public void example() {
    for (Integer port : cluster.getExposedPorts()) {
      try (Jedis jedis = new Jedis("localhost", port)) {
        System.err.println(jedis.clusterNodes());
      }
    }
  }

  @Test
  public void useClusterAwareClient() {
    JedisCluster jedis =
        new JedisCluster(new HostAndPort("localhost", cluster.getExposedPorts().get(0)));
    System.err.println(jedis.getClusterNodes());

    jedis.close();
  }

  @Test
  public void testMoved() {
    Jedis jedis = new Jedis("localhost", cluster.getExposedPorts().get(0), 100000);
    assertThatThrownBy(() -> jedis.set("a", "A"))
        .isInstanceOf(JedisMovedDataException.class)
        .hasMessageContaining("127.0.0.1");
  }

}
