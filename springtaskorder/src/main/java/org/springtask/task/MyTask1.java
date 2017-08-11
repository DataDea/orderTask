package org.springtask.task;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class MyTask1 extends QuartzJobBean {

    private Logger logger = Logger.getLogger(MyTask1.class);

    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        logger.debug("任务执行开始.....");
        System.out.println("我执行了.........");
        logger.debug("任务执行结束.......");
    }
}
