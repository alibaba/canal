package com.alibaba.otter.canal.deployer;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.alibaba.otter.canal.deployer.InstanceConfig.InstanceMode;

public class InstanceConfigTest {

    @Test
    public void testDefaultModeIsSpringWhenNotConfigured() {
        assertEquals(InstanceMode.SPRING, new InstanceConfig().getMode());
    }

    @Test
    public void testModeInheritsFromGlobalConfig() {
        InstanceConfig global = new InstanceConfig();
        global.setMode(InstanceMode.MANAGER);

        assertEquals(InstanceMode.MANAGER, new InstanceConfig(global).getMode());
    }

    @Test
    public void testInstanceModeOverridesGlobalConfig() {
        InstanceConfig global = new InstanceConfig();
        global.setMode(InstanceMode.MANAGER);

        InstanceConfig instance = new InstanceConfig(global);
        instance.setMode(InstanceMode.SPRING);

        assertEquals(InstanceMode.SPRING, instance.getMode());
    }
}
