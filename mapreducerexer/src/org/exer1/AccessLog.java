package org.exer1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class AccessLog {
    private static Logger logger = Logger.getLogger(AccessLog.class);

    public static class AccessLogMapper extends Mapper<Object, Text, Text, Text> {
        private Text line = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("{map获得数据:}=======>{key=====>}" + key + "{values=====>}" + value);
            line = value;
            if (line != null) {
                context.write(line, new Text(""));
            }
            logger.info("{map成功写入一个kv对}=====>");
        }
    }

    public static class AccessLogReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            logger.info("{reduec开始执行....}");
            logger.info("{reduce接到的数据为======>}=====>key" + key + "{======>value}" + values);
            context.write(key, new Text(""));
            logger.info("{reduce执行完成....}");
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String[] otherArgs = {"hdfs://192.168.109.129:9000/test3/words", "hdfs://192.168.109.129:9000/test2017"};
//        String[] otherArgs = {"hdfs://master:9000/test3/words", "hdfs://master:9000/wordexer"};
        Configuration conf = new Configuration();
        Job job = new Job(conf, "test");
        job.setJarByClass(AccessLog.class);
        job.setMapperClass(AccessLogMapper.class);
        job.setReducerClass(AccessLogReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        Boolean b = job.waitForCompletion(true);
        System.out.println(b ? 1 : 0);
    }
}
