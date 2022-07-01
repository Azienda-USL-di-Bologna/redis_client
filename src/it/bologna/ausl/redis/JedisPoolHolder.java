package it.bologna.ausl.redis;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 *
 * @author andrea
 */
public class JedisPoolHolder {

    private static volatile ConcurrentMap<String, JedisPool> jpm = new ConcurrentHashMap<>(5);
    private static final Object LOCK = new Object();
    
    
    protected JedisPoolHolder() {
    }

    public static JedisPool getInstance(String host, Integer port) {
        if (port == null || port == -1) {
            port = 6379;
        }
        String hashKey = host + ":" + port.toString();
        if (jpm.get(hashKey) == null) {
            synchronized (LOCK) {
                if (jpm.get(hashKey) == null) {
                    JedisPoolConfig jpc = new JedisPoolConfig();
                    jpc.setMaxTotal(50);

                    JedisPool tmp = new JedisPool(jpc, host, port);
                    jpm.put(hashKey, tmp);
                }
            }
        }
        return jpm.get(hashKey);
    }

}
