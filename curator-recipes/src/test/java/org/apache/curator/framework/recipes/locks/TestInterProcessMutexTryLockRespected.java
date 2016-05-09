/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.curator.framework.recipes.locks;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TestInterProcessMutexTryLockRespected extends TestInterProcessMutexBase
{
    private static final String LOCK_PATH = LOCK_BASE_PATH + "/our-lock";
    private static final long ACQUIRE_TIMEOUT_MILLIS = 3000;
    private static final long ACQUIRE_TIMEOUT_MILLIS_TOLERANCE = 100;
    private static final int RESOURCE_MAXIMUM_USAGE_MILLIS = 5000;
    private static final int RESOURCE_MINIMUM_USAGE_MILLIS = 1000;
    private static final int THREAD_POOL_SIZE = 1000;
    private static final int REQUEST_PER_SECONDS = 200;
    private static final int TEST_DURATION_SECONDS = 20;

    AtomicLong failedTimeout = new AtomicLong(0);



    @Override
    protected InterProcessLock makeLock(CuratorFramework client)
    {
        return new InterProcessMutex(client, LOCK_PATH);
    }

    @Test
    public void testTryLockRespected() throws Exception
    {
        //String connectString = "bfc-zookeeper-lock01.bravofly.intra:2181,bfc-zookeeper-lock02.bravofly.intra:2181,bfc-zookeeper-lock03.bravofly.intra:2181";
        String connectString = server.getConnectString();
        final CuratorFramework        client = CuratorFrameworkFactory.newClient(connectString, new RetryOneTime(1));
        ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(THREAD_POOL_SIZE * 20);
        final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE,
                                                                             1000,
                                                                             TimeUnit.MILLISECONDS,
                                                                             workQueue);

        try
        {
            client.start();
            client.blockUntilConnected();

            new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    for(int i=0; i<TEST_DURATION_SECONDS; i++)
                    {
                        System.out.println("...adding " + REQUEST_PER_SECONDS + " requests...");
                        for(int r=0; r < REQUEST_PER_SECONDS; r++)
                        {
                            threadPoolExecutor.execute(new TimeConsumingResource(client));

                        }
                        sleep(1000);
                    }
                }
            }).start();

            waitingAndShutdown(threadPoolExecutor);


        }
        finally
        {
            client.close();
        }

        Assert.assertEquals(failedTimeout.get(), 0);

    }

    public void waitingAndShutdown(ThreadPoolExecutor threadPoolExecutor)
    {
        for(int i=0; i<TEST_DURATION_SECONDS+1; i++)
        {
            sleep(1000);
        }
        threadPoolExecutor.shutdown();
        while (!threadPoolExecutor.isTerminated())
        {
            sleep(100);
        }
    }

    private class TimeConsumingResource implements Runnable
    {
        private final CuratorFramework client;

        public TimeConsumingResource(CuratorFramework client)
        {
            this.client = client;
        }
        @Override
        public void run()
        {
            try
            {
                InterProcessLock lock = makeLock(client);
                long startAt = new Date().getTime();
                boolean acquired = lock.acquire(ACQUIRE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                long duration = new Date().getTime() - startAt;
                if(acquired)
                {
                    sleep(RESOURCE_MAXIMUM_USAGE_MILLIS, RESOURCE_MINIMUM_USAGE_MILLIS);
                    lock.release();
                }
                if(duration > ACQUIRE_TIMEOUT_MILLIS+ACQUIRE_TIMEOUT_MILLIS_TOLERANCE)
                {
                    System.out.println(String.format("Wrong! Lock %s acquired in %s", (acquired?"     ":" NOT "), duration));
                    failedTimeout.incrementAndGet();
                }
            }
            catch(Exception ex)
            {
                ex.printStackTrace();
            }
        }
    }


    private void sleep(int sleepMillis)
    {
        sleep(sleepMillis+1, sleepMillis);
    }
    private void sleep(int sleepMaximumMillis, int sleepMinimumMillis)
    {
        try
        {
            Thread.sleep(new Random().nextInt(sleepMaximumMillis-sleepMinimumMillis) + sleepMinimumMillis);
        }
        catch (InterruptedException e)
        {
        }
    }
}
