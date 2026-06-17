package com.alibaba.otter.canal.meta;

import com.alibaba.otter.canal.protocol.position.PositionRange;
import org.junit.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.List;
import java.util.Map;

@Ignore
public class RedisMetaManagerTest extends AbstractMetaManagerTest {

    private JedisPool jedisPool;

    private String host="127.0.0.1";
    private Integer port=6379;

    @Before
    public void setUp(){
        jedisPool=new JedisPool(host,port);
        clean();
    }

    @After
    public void tearDown(){
        clean();
        jedisPool.close();
    }

    public void clean (){
        Jedis jedis= jedisPool.getResource();
        ScanParams scanParams=new ScanParams();
        scanParams.match("otter_canal_meta*");
        scanParams.count(1000);
        ScanResult<String> scanResult;
        String coursor="0";
        do {
            scanResult = jedis.scan(coursor, scanParams);
            List<String> keys=scanResult.getResult();
            for(String key:keys){
                jedis.del(key);
            }
            coursor=scanResult.getStringCursor();
        }while(Long.parseLong(scanResult.getStringCursor())>0);
        jedis.close();
    }


    @Test
    public void testSubscribeAll() {
        RedisMetaManager metaManager = new RedisMetaManager();
        metaManager.setRedisHost(host);
        metaManager.setRedisPort(port);
        metaManager.start();
        doSubscribeTest(metaManager);
        
    }

    @Test
    public void testBatchAll() {
        RedisMetaManager metaManager = new RedisMetaManager();
        metaManager.setRedisHost(host);
        metaManager.setRedisPort(port);
        metaManager.start();

        doBatchTest(metaManager);

        metaManager.clearAllBatchs(clientIdentity);
        Map<Long, PositionRange> ranges = metaManager.listAllBatchs(clientIdentity);
        Assert.assertEquals(0, ranges.size());
        
    }

    @Test
    public void testCursorhAll() {
        RedisMetaManager metaManager = new RedisMetaManager();
        metaManager.setRedisHost(host);
        metaManager.setRedisPort(port);
        metaManager.start();
        
        doCursorTest(metaManager);
        
    }
}
