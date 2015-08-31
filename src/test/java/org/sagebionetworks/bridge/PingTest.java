package org.sagebionetworks.bridge;

import org.junit.Assert;
import org.junit.Test;

public class PingTest {
    @Test
    public void test() {
        Ping ping = new Ping();
        Assert.assertEquals("pong", ping.ping());
    }
}
