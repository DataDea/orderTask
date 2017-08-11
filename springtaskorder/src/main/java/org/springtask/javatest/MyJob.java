package org.springtask.javatest;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Date;

public class MyJob implements Job {
    private Logger logger = Logger.getLogger(MyJob.class);
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        logger.debug("{任务开始被调度了.......}");
        System.out.println("任务调度完成=====>"+new Date());
        logger.debug("{任务调度结束了.........}");
    }
}
