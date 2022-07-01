package it.bologna.ausl.redis.redispubsub;

import it.bologna.ausl.redis.JedisPoolHolder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author andrea
 */
public class Publisher {

    private final JedisPool jp;

    public Publisher(String host, int port) {
        jp = JedisPoolHolder.getInstance(host, port);
    }

    public void publish(String channel, String message) {
        try (Jedis j = jp.getResource()) {
            j.publish(channel, message);
        }
    }

}
