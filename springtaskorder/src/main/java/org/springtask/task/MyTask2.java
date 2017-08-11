package org.springtask.task;

import org.apache.log4j.Logger;
import org.springtask.interfs.TaskService;

public class MyTask2 implements TaskService {
    private Logger logger = Logger.getLogger(MyTask2.class);
    public void doTask(){
        logger.debug("{任务调度开始.....}");
        System.out.println("我的任务执行完了.....");
        logger.debug("{任务调度结束了.....}");
    }
}
