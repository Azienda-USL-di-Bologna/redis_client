package it.bologna.ausl.redis.redispubsub;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import redis.clients.jedis.JedisPubSub;

/**
 *
 * @author andrea
 */
public class BabelJedisSubscriber extends JedisPubSub {

    private String lastMessage;
    private final String channel;
    private final Lock lock = new ReentrantLock();
    private final Lock sleepLock = new ReentrantLock();
    private final Condition sleepCondition = sleepLock.newCondition();
    //Flag per eseguire una sola volta l'unsubscribe
    //(alrimenti viene chiamato sia da chi riceve il messaggio e dal thread del timeout)
    //causando il famigerato effetto PARMA :D

    private boolean alreadyUnsub = false;

    BabelJedisSubscriber(String channel) {
        this.channel = channel;

    }

    public void sleep(int timeoutSec) {
        sleepLock.lock();
        try {
            while (true) {
                try {
                    sleepCondition.await(timeoutSec, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {

                }
                break;
            }
        } finally {
            sleepLock.unlock();
        }

    }

    @Override
    public void onMessage(String channel, String message) {
        super.onMessage(channel, message);
        lastMessage = message;
        unsubscribe();
    }

    @Override
    public void unsubscribe() {
        lock.lock();
        try {
            //controlliamo di essere sottoscritti e di non aver già annullato la sottoscrizione
            if (isSubscribed() && !alreadyUnsub) {
                super.unsubscribe();
                alreadyUnsub = true;
            }
            sleepLock.lock();
            sleepCondition.signalAll();
        } finally {
            lock.unlock();
            sleepLock.unlock();
        }

    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
        super.onUnsubscribe(channel, subscribedChannels);
    }

    public String getLastMessage() {
        return lastMessage;
    }

    public String getChannel() {
        return channel;
    }

}
