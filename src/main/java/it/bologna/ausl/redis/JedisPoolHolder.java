package it.bologna.ausl.redis;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 *
 * @author andrea
 */
public class JedisPoolHolder {

    private static volatile ConcurrentMap<String, JedisPool> jpm = new ConcurrentHashMap<>(5);
    private static final Object LOCK = new Object();
    
    
    protected JedisPoolHolder() {
    }

    public static JedisPool getInstance(String host, Integer port, int db, String password) {
        if (port == null || port == -1) {
            port = 6379;
        }
        if (password != null && password.isEmpty())
            password = null;
        String hashKey = host + ":" + port.toString() + ":" + db;
        if (jpm.get(hashKey) == null) {
            synchronized (LOCK) {
                if (jpm.get(hashKey) == null) {
                    JedisPoolConfig jpc = new JedisPoolConfig();
                    jpc.setMaxTotal(200);
                    JedisPool tmp = new JedisPool(jpc, host, port, Protocol.DEFAULT_TIMEOUT, password, db);
                    jpm.put(hashKey, tmp);
                }
            }
        }
        return jpm.get(hashKey);
    }

}
