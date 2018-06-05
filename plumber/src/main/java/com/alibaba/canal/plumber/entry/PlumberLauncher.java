package com.alibaba.canal.plumber.entry;

import com.alibaba.canal.plumber.stage.StageController;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 管道工go go go
 * @author dsqin
 * @date 2018/6/5
 */
public class PlumberLauncher {

    private static final Logger logger = LoggerFactory.getLogger(PlumberLauncher.class);

    public static void main(String[] args)
    {
        final StageController controller = ContextLocator.getController();
        controller.start();
        try
        {
            logger.info("INFO ## the plumber server is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread()
            {
                public void run()
                {
                    try
                    {
                        PlumberLauncher.logger.info("INFO ## stop the plumber server");
                        controller.stop();
                    }
                    catch (Throwable e)
                    {
                        PlumberLauncher.logger.warn("WARN ##something goes wrong when stopping plumber Server:\n{}",
                                ExceptionUtils.getFullStackTrace(e));
                    }
                    finally
                    {
                        PlumberLauncher.logger.info("INFO ## plumber server is down.");
                    }
                }
            });
        }
        catch (Throwable e)
        {
            logger.error("ERROR ## Something goes wrong when starting up the plumber Server:\n{}",
                    ExceptionUtils.getFullStackTrace(e));
            System.exit(0);
        }
    }
}
