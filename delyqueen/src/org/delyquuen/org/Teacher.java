package org.delyquuen.org;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;

/**
 * Created by yh on 17-7-18.
 */
class Teacher implements Runnable{
    private DelayQueue<Student> students;
    private ExecutorService exec;

    public Teacher(DelayQueue<Student> students,ExecutorService exec) {
        super();
        this.students = students;
        this.exec = exec;
    }


    @Override
    public void run() {
        try {
            System.out.println("考试开始……");
            while (!Thread.interrupted()) {
                students.take().run();
            }
            System.out.println("考试结束……");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}