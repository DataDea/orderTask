package org.exer3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SortMapReduce {
    private static Logger logger = Logger.getLogger(SortMapReduce.class);

    static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private static IntWritable data = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("{map开始执行.....}====>");
            String line = value.toString();
            logger.info("{map取到的value为:}====>" + line);
            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable());
            logger.info("{map写入数据成功!!!}");
        }
    }


    static class SortReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private static IntWritable intWritable = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            logger.info("{reduce开始运算...}====>");
            for (IntWritable val : values) {
                logger.info("{迭代器的值为======>}" + val);
                context.write(intWritable, key);
                logger.info("{reduce执行成功====>}"+intWritable);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String[] otherArgs = {"hdfs://192.168.109.129:9000/test2/sortfile", "hdfs://192.168.109.129:9000/sort"};
        logger.info("{组初始化的出纳和素=====>>>}"+otherArgs[0]+"======"+otherArgs[1]);
        Configuration configuration = new Configuration();
        Job job = new Job(configuration);
        job.setJarByClass(SortMapReduce.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
        System.out.println("Finished");
    }


}
