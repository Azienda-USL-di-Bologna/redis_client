package it.bologna.ausl.redis.redispubsub;

import it.bologna.ausl.redis.JedisPoolHolder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author andrea
 */
public class Subscriber {

    private final JedisPool jp;

    public Subscriber(String host, int port) {
        jp = JedisPoolHolder.getInstance(host, port);

    }

    public String waitResult(String channel, int timeoutSec) {
        try (Jedis j = jp.getResource()) {
            BabelJedisSubscriber bps = new BabelJedisSubscriber(channel);
            if (timeoutSec != 0) {
                Runnable timeoutThread = () -> {
                    bps.sleep(timeoutSec);
                    bps.unsubscribe();
                };
                new Thread(timeoutThread).start();
            }
            j.subscribe(bps, channel);
            return bps.getLastMessage();
        }
    }

}
