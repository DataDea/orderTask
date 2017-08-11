package org.exer4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class TableAloneLink {

    public static int time = 0;
    private static Logger logger = Logger.getLogger(TableAloneLink.class);

    static class LinKMapper extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * map的主要任务是将输入分割成child和parent,然后正反输出一次作为右表和左表
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String childname = new String();
            String parentNmae = new String();
            String relationtype = new String();
            String line = value.toString();
            logger.info("{Map的输入为:}======>" + line);
            int i = 0;
            while (line.charAt(i) != ' ') {
                i++;
            }
            String[] values = {line.substring(0, i), line.substring(i + 1)};
            logger.info("{字符串数组的值为:====>}" + values[0] + "======" + values[1]);
            if (values[0].compareTo("child") != 0) {
                childname = values[0];
                parentNmae = values[1];
                relationtype = "1";
                //左表
                context.write(new Text(values[1]), new Text(relationtype + "+" + childname + "+" + parentNmae));
                //右表
                relationtype = "2";
                context.write(new Text(values[0]), new Text(relationtype + "+" + childname + "+" + parentNmae));
            }
        }
    }

    static class LinkReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //输出表头
            if (time == 0) {
                context.write(new Text("grandChild"), new Text("grandparent"));
                time++;
            }
            int grandChildnum = 0;
            String grandchild[] = new String[10];
            int grandparentnum = 0;
            String grandparen[] = new String[10];
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                String record = iterator.next().toString();
                int len = record.length();
                int i = 2;
                if (len == 0)
                    continue;
                char relationtype = record.charAt(0);
                String childname = new String();
                String parentname = new String();
                //取孩子的名字
                while (record.charAt(i) != '+') {
                    childname = childname + record.charAt(i);
                    i++;
                }
                i = i+1;
                //去取parent的名字
                while (i < len) {
                    parentname = parentname + record.charAt(i);
                    grandChildnum++;
                }
                if (relationtype == '1') {
                    grandchild[grandChildnum] = childname;
                    grandChildnum++;
                } else {
                    grandparen[grandparentnum] = parentname;
                    grandparentnum++;
                }
            }
            //利用循环往里面写东西
            if (grandparentnum != 0 && grandChildnum != 0) {
                for (int m = 0; m < grandChildnum; m++) {
                    for (int n = 0; n < grandparentnum; n++) {
                        context.write(new Text(grandchild[m]), new Text(grandparen[n]));
                    }
                }
            }

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String[] otherArgs = {"hdfs://192.168.109.129:9000/test2/alonelinktable", "hdfs://192.168.109.129:9000/alonelinktable"};
        logger.info("{组初始化的出纳和素=====>>>}" + otherArgs[0] + "======" + otherArgs[1]);
        Configuration configuration = new Configuration();
        Job job = new Job(configuration);
        job.setJarByClass(TableAloneLink.class);
        job.setMapperClass(LinKMapper.class);
        job.setReducerClass(LinkReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
        System.out.println("Finished");
    }
}
