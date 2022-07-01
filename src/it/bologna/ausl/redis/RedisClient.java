package it.bologna.ausl.redis;

import java.util.List;
import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class RedisClient {

    String host;
    Integer port;
    String qout;
    String qin;
    JedisPool jp;

    /*
     jasperjob={
     'appID':"rediscli",
     'jobID':"1",
     'jobList':[ {
     'jobN':"job1",
     'jobType':"Jasper",
     'params': {
     'templatePath':"/templates/jasper/reportFrontespizio.jrxml",
     'data':"data",
     'alignment':"1",
     }
     }],
     'returnQueue':"queueName"
     }
     */

    /*
     stampaunica={
     'appID':"rediscli",
     'jobID':"1",
     'jobList':[{
     'jobN':"job1",
     'jobType':"StampaUnica",  
     'params':{
     'savePath':"/test/stampaUnicissima.pdf",
     'jasperList':[{
     'filePath':"/test/deminchia_frontespizio.pdf",
     'jasperParams':{    	        
     'templatePath':"/templates/jasper/reportFrontespizio.jrxml",
     'data':"data",
     'alignment':"1"
     }
     },
     {
     'filePath':"/test/deminchia_frontespizio2.pdf",
     'jasperParams':{    	        
     'templatePath':'/templates/jasper/reportFrontespizio.jrxml',
     'data':data,
     'alignment':"1",
     }
     }],
     'allegati':["5084fe42b2e3c615ff788452","50811278b2e34f8c816881e3"]
     }
     }],
     'returnQueue':"queueName"
     }

     */
    /* 
     updatebabeljob={
     'appID':"rediscli",
     'jobID':"1",
     'jobList':[ {
     'jobN':"job1",
     'jobType':"UpdateBabel",
     'params': {
     'idApplicazione': idApplicazione
     'tokenApplicazione': tokenApplicazione
     'setAttivita': setAttivita
     'archiviazione': archiviazione 
     'accessoEsclusivo': accessoEsclusivo
     'actionType': (insert|update|delete|update_mult|update_per_idesterno|delete_mult|delete_per_idesterno|allinea)
                    
     'listaAttivita' : [
     'idAttivita': idattivita
     'idEsterno': idEsterno
     'idUtente': idUtente
     'tipoAttivita': tipoAttivita
     'idTipiAttivita': tipoAttivita
     'descrizioneAttivita': descrizioneAttivita
     'oggettoAttivita': oggettoAttivita
     'provenienza': provenienza
     'priorita':  priorita
     'labelUrlCommand': labelUrlCommand
     'urlCommand': urlCommand
     'labelUrlCommand2': labelUrlCommand2
     'urlCommand2': urlCommand2
     'labelUrlCommand3': labelUrlCommand3
     'urlCommand3': urlCommand3                    
     'noteAttivita': noteAttivita      
     'uuidAnteprima': uuidAnteprima
     ]
     }
     }],
     'returnQueue':"queueName"
     }
     * Cose da ricordare: 
     * in un insert i parametri obbligatori sono: idAttivita, idEsterno, idUtente
     * in un update e delete (update/delete per idAttivita) i parametri obbligatori sono: idAttivita
     * in un update_mult e delete_mult (update/delete con like per idAttivita) i parametri obbligatori sono: idAttivita
     * in un update_per_idesterno e delete_per_idesterno (update/delete per idEsterno) i parametri obbligatori sono: idEsterno
     * in un allinea i parametri obbligatori sono: (nelle attività, almeno una e in ognuna idAttivita, idEsterno e IdUtente), setattivita, archiviazione
     */


    public RedisClient(String host, Integer port, String qout, String qin) {
        this.qout = qout;
        this.qin = qin;
        if (port != null && port != -1) {
            this.port = port;
        } else {
            port = 6379;
        }
        jp = JedisPoolHolder.getInstance(host, port);
    }

    public RedisClient(String host, Integer port, String qin) {
        this(host, port, null, qin);
    }
    
    public RedisClient(String host, Integer port) {
        this(host, port, null, null);
    }

    /**
     * ritorna tutti i valori all'interno della lista di default
     *
     * @return tutti i valori all'interno della lista di default
     */
    public List<String> allValues() {
        return values(0, -1);
    }

    /**
     * ritorna tutti i valori all'interno di una list specificata
     *
     * @param queue nome della list
     * @return tutti i valori all'interno di una list specificata
     */
    public List<String> allValues(String queue) {
        return values(queue, 0, -1);
    }

    /**
     * ritorna i valori in un certo intervallo nella list di default
     *
     * @param start
     * @param end
     * @return i valori in un certo intervallo nella list di default
     */
    public List<String> values(long start, long end) {
        return values(qin, 0, -1);
    }

    /**
     * ritorna i valori in un certo intervallo nella list specificata
     *
     * @param queue nome della list
     * @param start
     * @param end
     * @return i valori in un certo intervallo nella list specificata
     */
    public List<String> values(String queue, long start, long end) {
        try (Jedis j = jp.getResource()) {
            return j.lrange(queue, start, end);
        } 
    }

    /**
     * ritorna il valore in un certo indice nella list di default
     *
     * @param index
     * @return il valore in un certo indice nella list di default
     */
    public String value(long index) {
        return value(qin, index);
    }

    /**
     * ritorna il valore in un certo indice nella list specificata
     *
     * @param queue nome della list
     * @param index
     * @return il valore in un certo indice nella list specificata
     */
    public String value(String queue, long index) {
        try (Jedis j = jp.getResource()) {

            return j.lindex(queue, index);
        }
    }

    /**
     * inserisce il valore passato a sinistra nella list di default
     *
     * @param data
     * @return true se il valore è inserito, false altrimenti
     */
    public boolean put(String data) {
        try (Jedis j = jp.getResource()) {
            return (j.lpush(qin, data) > 0);
        }
    }

    /**
     * inserisce il valore passato a sinistra nella list specificata
     *
     * @param data
     * @param queue nome della list
     * @return true se il valore è inserito, false altrimenti
     */
    public boolean put(String data, String queue) {
        try (Jedis j = jp.getResource()) {
            return (j.lpush(queue, data) > 0);
        }
    }

    /**
     * inserisce un valore da destra nella list di default
     *
     * @param data
     * @return true se il valore è inserito, false altrimenti
     */
    public boolean rpush(String data) {
        try (Jedis j = jp.getResource()) {
            return (j.rpush(qin, data) > 0);
        }
    }

    /**
     * inserisce un valore da destra nella list specificata
     *
     * @param data
     * @param queue nome della list
     * @return true se il valore è inserito, false altrimenti
     */
    public boolean rpush(String data, String queue) {
        try (Jedis j = jp.getResource()) {
            return (j.rpush(queue, data) > 0);
        }
    }

    /**
     * etrae un valore da destra dalla list di default e in caso di list vuota
     * rimane in attesa di un valore
     *
     * @return il valore estratto

     */
    public String bpop()  {
        return bpop(qin);
    }

    /**
     * etrae un valore da destra dalla list specificata e in caso di list vuota
     * rimane in attesa di un valore
     *
     * @param queue nome della list
     * @return il valore estratto
     */
    public String bpop(String queue) {
        try (Jedis j = jp.getResource()) {
            return j.brpop(0, queue).get(1);
        }
    }
    
    /**
     * etrae un valore da destra dalla list specificata e in caso di list vuota
     * rimane in attesa di un valore fino allo scadere del timeout
     *
     * @param queue nome della list
     * @param timeout tempo massimo di attesa del valore in secondi
     * @return il valore estratto o null in caso di scadenza del timeout
     */
    public String bpop(String queue, int timeout) {

    try (Jedis j = jp.getResource()) {
            List<String> res = j.brpop(timeout, queue);
            if (res != null)
                return res.get(1);
            else
                return null;
        }
    }

    /**
     * torna l'elemento più a destra della list di default
     *
     * @return l'elemento più a destra della list di default
     */
    public String get() {
        return get(qin);
    }

    /**
     * torna l'elemento più a destra della list specificata
     *
     * @param queue nome della list
     * @return l'elemento più a destra della list specificata
     */
    public String get(String queue) {
        try (Jedis j = jp.getResource()) {
            return j.lrange(queue, -1, -1).get(0);
        }
    }
    
    /**
     * torna il valore della chiave passata
     *
     * @param key nome della list
     * @return il valore della chiave passata
     */
    public String getKey(String key) {
        try (Jedis j = jp.getResource()) {
            return j.get(key);
        }
    }

    /**
     * toglie e ritorna l'elemento in testa alla list passata
     *
     * @param queue nome della list
     * @return l'elemento in testa alla list passata dopo averlo rimosso
     */
    public String pop(String queue) {
        try (Jedis j = jp.getResource()) {
            return j.rpop(queue);
        }
    }

    /**
     * elimina uno o più valori a partire da sinistra nella list specificata
     *
     * @param queue nome della list
     * @param value valore da rimuovere
     * @return il numero di valori rimossi
     */
    public long lrem(String queue, String value) {
        try (Jedis j = jp.getResource()) {
            return j.lrem(queue, 0, value);
        }
    }

    /**
     * inserisce il valore passato a sinistra nelle list di ouput di default ed
     * estrae quello più a destra nella coda di input di default
     *
     * @param data
     * @return il valore più a destra della list di input di default
     */
    public String putpop(String data)  {
        return putpop(data, qout, qin);
    }

    /**
     * inserisce il valore passato a sinistra nelle list di ouput specificata ed
     * estrae quello più a destra nella coda di input specificata
     *
     * @param data
     * @param qout nome della list di output
     * @param qin nome della list di input
     * @return il valore più a destra della list di input specificata
     */
    public String putpop(String data, String qout, String qin)  {
        put(data, qout);
        return bpop(qin);
    }

    /**
     * estrae atomicamente il valore più a destra della list sorgente e lo
     * inserisce a sinistra nella list di destinazione; in caso di list sorgente
     * vuota rimane in attesa di un valore fino allo scadere del timeout
     *
     * @param qsource nome della list sorgente
     * @param qdest nome della list di destinazione
     * @param timeout timeout di attesa
     * @return il valore estratto dalla list sorgente
     */
    public String brpoplpush(String qsource, String qdest, int timeout) {
        try (Jedis j = jp.getResource()) {
            return j.brpoplpush(qsource, qdest, timeout);
        }
    }

    /**
     * estrae atomicamente il valore più a destra della list sorgente e lo
     * inserisce a sinistra nella list di destinazione; in caso di list sorgente
     * vuota rimane in attesa di un valore
     *
     * @param qsource nome della list sorgente
     * @param qdest nome della list di destinazione
     * @return il valore estratto dalla list sorgente
     */
    public String brpoplpush(String qsource, String qdest) {
        return brpoplpush(qsource, qdest, 0);
    }

    /**
     * estrae atomicamente il valore più a destra della list sorgente e lo
     * inserisce a sinistra nella list di destinazione;
     *
     * @param qsource nome della list sorgente
     * @param qdest nome della list di destinazione
     * @return il valore estratto dalla list sorgente
     */
    public String rpoplpush(String qsource, String qdest) {
        try (Jedis j = jp.getResource()) {
            return j.rpoplpush(qsource, qdest);
        }
    }

    /**
     * estrae atomicamente il valore più a sinistra della list sorgente e lo
     * inserisce a sinistra nella list di destinazione;
     *
     * @param qsource nome della list sorgente
     * @param qdest nome della list di destinazione
     * @return il valore estratto dalla list sorgente
     */
    public String lpoplpush(String qsource, String qdest) {
        try (Jedis j = jp.getResource()) {
            boolean exit = true;
            String res = null;
            String watch = j.watch(qsource, qdest);
            List<String> lpop = j.lrange(qsource, 0, 0);
            do {
                if (lpop == null || lpop.isEmpty()) {
                    j.unwatch();
                } else {
                    Transaction multi = j.multi();
                    multi.lpop(qsource);
                    res = lpop.get(0);
                    multi.lpush(qdest, res);
                    List<Object> exec = multi.exec();
                    if (exec == null) {
                        exit = false;
                    }
                }
            } while (!exit);
            return res;
        }
    }

    /**
     * estrae atomicamente tutti i valori della list sorgente e li inserisce a
     * destra nella list di destinazione
     *
     * @param qsource nome della list sorgente
     * @param qdest nome della list di destinazione
     * @return i valori estratti dalla list sorgente
     */
    public List<String> lpoprpushAll(String qsource, String qdest) {
        try (Jedis j = jp.getResource()) {
            boolean exit;
            List<String> lpop = null;
            do {
                exit = true;
                String watch = j.watch(qsource, qdest);
                lpop = j.lrange(qsource, 0, -1);
                if (lpop == null || lpop.isEmpty()) {
                    j.unwatch();
                } else {
                    Transaction multi = j.multi();
                    multi.del(qsource);
//                lpopValue = lpop.get(0);
                    for (String s : lpop) {
                        multi.rpush(qdest, s);
                    }
                    List<Object> exec = multi.exec();
                    if (exec == null) {
                        exit = false;
                    }
                }
            } while (!exit);
            return lpop;
        }
    }

    /**
     * cerca una stringa secondo una espressione regolare all'interno degli
     * elementi di una list
     *
     * @param queue la list in cui cercare
     * @param regex l'espressione regolare da soddisfare
     * @return true se esiste almeno un elemento nella list che soddisfa
     * l'espressione passata, false altrimenti
     */
    public boolean find(String queue, String regex) {
        List<String> values = values(queue, 0, -1);
        boolean found = false;
        for (String value : values) {
            System.out.println(value);
            if (value.matches(regex)) {
                found = true;
                break;
            }
        }
        return found;
    }

    /**
     * torna la lunghezza della list di default
     *
     * @return lunghezza della list di default
     */
    public long len() {
        try (Jedis j = jp.getResource()) {
            return j.llen(qin);
        }
    }

    /**
     * torna la lunghezza della list specificata
     *
     * @param queue nome della list
     * @return lunghezza della list specificata
     */
    public long len(String queue) {
        try (Jedis j = jp.getResource()) {
            return j.llen(queue);
        }
    }

    /**
     * torna l'elenco di tutte le chiavi secondo un pattern
     *
     * @param pattern
     * @return
     */
    public Set<String> list(String pattern) {
        try (Jedis j = jp.getResource()) {
            return j.keys(pattern);
        }
    }
    
    /**
     * setta la scadenza di una chiave
     *
     * @param key
     * @param seconds
     * @return 1 se tutto OK 0 altrimenti
     */
    public Long expire(String key, int seconds) {
        try (Jedis j = jp.getResource()) {
            return j.expire(key, seconds);
        }        
    }
    
    /**
     * setta la scadenza di una chiave
     *
     * @param key
     * @param seconds
     * @return 1 se tutto OK 0 altrimenti
     * @throws java.lang.Exception
     */
    public Long expire(byte[] key, int seconds) throws Exception {
        try (Jedis j = jp.getResource()) {
            return j.expire(key, seconds);
        }
    }  

    /** controlla se la chiave è scaduta
     * La funzione controlla prima l'esistenza della chiave, se esiste controlla se il valore passato è uguale al valore della chiave. 
     * Inoltre se la chiave esiste ne prolunga la scadenza del numero di secondi passati
     * 
     * @param key
     * @param value 
     * @param secondsToExtend numero di secondi del quale prolungare la scadenza della chiave
     * * passare -1 per non prolungare la scadenza, 
     * * passare 0 per far scadere subito la chiave, 
     * * passare -2 per disattivare la scadenza
     * @return "true" se la chiave è scaduta, "false" altrimenti
     */
    public boolean isExpired(String key, String value, int secondsToExtend) {
        boolean res = true;
        try (Jedis j = jp.getResource()) {
            String watch = j.watch(key);
            Transaction multi = j.multi();
            
            // salvo il timeout corrente per ripristinarlo in caso la chiave sia scaduta
            Response<Long> responseCurrentTtl = multi.ttl(key);
            if (secondsToExtend == -2) {
                multi.persist(key);
            }
            else if (secondsToExtend != -1) {
                multi.expire(key, secondsToExtend);
            }
            Response<String> responseGet = multi.get(key);
            List<Object> exec = multi.exec();
            if (exec != null) {
                String keyValue = responseGet.get();
                res = !value.equals(keyValue);
                
                // se la chiave è scaduta e c'era un timeout, ri-setto il timeout originale
                int currentTtl = responseCurrentTtl.get().intValue();
                if (res && currentTtl != -1) {
                    watch = j.watch(key);
                    multi = j.multi();
//                    System.out.println("val: " + responseCurrentTtl.get().intValue());
                    multi.expire(key, currentTtl);
                    exec = multi.exec();
                }
            }
            return res;        
        }
    }
    
    /**
     * torna il numero di millisecondi rimanenti alla chiave prima di essere eliminata
     *
     * @param key il nome della chiave da eliminare
     * @return il numero di millisecondi rimanenti alla chiave prima di essere eliminata
     */
    public long getKeyTimeout(String key) {
        try (Jedis j = jp.getResource()) {
            return j.ttl(key);
        }
    }
    
    /**
     * elimina una list o altra chiave
     *
     * @param queue il nome della chiave da eliminare
     * @return in numero di elementi eliminati
     */
    public long del(String queue) {
        try (Jedis j = jp.getResource()) {
            return j.del(queue);
        }
    }

    /**
     * setta un valore String nella chiave specificata
     *
     * @param key chiave
     * @param value valore
     * @return OK o Error
     */
    public String set(String key, String value) {
        try (Jedis j = jp.getResource()) {
            return j.set(key, value);
        }
    }

    /**
     * setta un valore byte nella chiave specificata
     *
     * @param key chiave
     * @param value valore
     * @return OK o Error
     */
    public String set(String key, byte[] value) {
        try (Jedis j = jp.getResource()) {
            return j.set(key.getBytes(), value);
        }
    }

    /**
     * setta un valore String nella chiave specificata con un determinato
     * timeout
     *
     * @param key chiave
     * @param value valore
     * @param seconds timeout in secondi
     * @return OK o Error
     */
    public String setex(String key, String value, int seconds) {
        try (Jedis j = jp.getResource()) {
            return j.setex(key, seconds, value);
        }
    }

    /**
     * setta un valore byte nella chiave specificata con un determinato timeout
     *
     * @param key chiave
     * @param value valore
     * @param seconds timeout in secondi
     * @return OK o Error
     */
    public String setex(String key, byte[] value, int seconds) {
        try (Jedis j = jp.getResource()) {
            return j.setex(key.getBytes(), seconds, value);
        }
    }

    /**
     * setta un valore String nella chiave specificata solo se non esiste
     *
     * @param key chiave
     * @param value valore
     * @return 1 se la chiave non esisteva ed il valore è stato settato, 0 altirmenti
     */
    public Long setnx(String key, String value) {
        try (Jedis j = jp.getResource()) {
            return j.setnx(key, value);
        }
    }

    /**
     * setta un valore String nella chiave specificata solo se non esiste
     *
     * @param key chiave
     * @param value valore
     * @return 1 se la chiave non esisteva ed il valore è stato settato, 0 altirmenti
     */
    public Long setnx(String key, byte[] value) {
        try (Jedis j = jp.getResource()) {
            return j.setnx(key.getBytes(), value);
        }
    }
    
    /**
     * setta un valore String in un campo di una chiave hash specificata
     *
     * @param key chiave
     * @param field campo
     * @param value valore
     * @return 1 se il valore è settato, 0 altrimenti
     */
    public Long hset(String key, String field, String value) {
        try (Jedis j = jp.getResource()) {
            return j.hset(key, field, value);
        }
    }

    /**
     * setta un valore byte in un campo di una chiave hash specificata
     *
     * @param key chiave
     * @param field campo
     * @param value valore
     * @return 1 se il valore è settato, 0 altrimenti
     */
    public Long hset(String key, String field, byte[] value) {
        try (Jedis j = jp.getResource()) {
            return j.hset(key.getBytes(), field.getBytes(), value);
        }
    }

    /**
     * torna il valore String del campo specificato della chiave passata
     *
     * @param key chiave
     * @param field campo
     * @return il valore String del campo specificato della chiave passata
     */
    public String hget(String key, String field) {
        try (Jedis j = jp.getResource()) {
            return j.hget(key, field);
        }
    }

    /**
     * torna il valore byte del campo specificato della chiave passata
     *
     * @param key chiave
     * @param field campo
     * @return il valore byte del campo specificato della chiave passata
     */
    public byte[] hgetBytes(String key, String field) {
        try (Jedis j = jp.getResource()) {
            return j.hget(key.getBytes(), field.getBytes());
        }
    }
    
    /**
     * torna l'elenco dei campi della chiave
     * 
     * @param key chiave
     * @return l'elenco dei campi della chiave
     */
    public Set<String> hkeys(String key) {
        try (Jedis j = jp.getResource()) {
            return j.hkeys(key);
        } 
    }
    
    /**
     * elimina il campo specificato della chiave passata
     *
     * @param key chiave
     * @param field campo
     * @return 1 se il valore era presente ed è stato eliminato, 0 altrimenti
     */
    public Long hdel(String key, String field) {
        try (Jedis j = jp.getResource()) {
            return j.hdel(key, field);
        }
    }
}
