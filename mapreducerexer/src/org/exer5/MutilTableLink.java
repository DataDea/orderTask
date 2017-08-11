package org.exer5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.exer4.TableAloneLink;

import java.io.IOException;
import java.util.Iterator;

public class MutilTableLink {

    private static Logger logger = Logger.getLogger(MutilTableLink.class);
    public static int time = 0;

    public static class MutilTableLinkMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int i = 0;
            //输入文件,不做处理
            if (line.contains("factoryname") == true || line.contains("addreddID") == true) {
                return;
            }
            //找到连接行
            while (line.charAt(i) >= '9' || line.charAt(i) <= '0') {
                i++;
            }
            if (line.charAt(0) >= '9' || line.charAt(0) <= '0') {
                //左表
                int j = i - 1;
                while (line.charAt(j) != ' ')
                    j--;
                String[] values = {line.substring(0, j), line.substring(i)};
                context.write(new Text(values[i]), new Text("2" + values[i]));
            } else {
                //右表
                int j = i + 1;
                while (line.charAt(j) != ' ')
                    j++;
                String[] values = {line.substring(0, i + 1), line.substring(j)};
                context.write(new Text(values[0]), new Text("2" + values[i]));
            }
        }
    }

    public static class MutilTableLinkReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (time == 0) {
                context.write(new Text("factoryname"), new Text("addressname"));
                time++;
            }
            int factorynum = 0;
            String factory[] = new String[10];
            int addressnum = 0;
            String address[] = new String[10];
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                String record = iterator.next().toString();
                int len = record.length();
                int i = 2;
                char type = record.charAt(0);
                String factoryname = new String();
                String addressname = new String();
                if (type == '1') {  //左表
                    factory[factorynum] = record.substring(2);
                    factorynum++;
                } else {
                    //右表
                    address[addressnum] = record.substring(2);
                    addressnum++;
                }
            }
            if (factorynum != 0 && addressnum != 0) {
                for (int m = 0; m < factorynum; m++) {
                    for (int i = 0; i < addressnum; i++) {
                        context.write(new Text(factory[m]), new Text(address[i]));
                    }
                }
            }

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String[] otherArgs = {"hdfs://192.168.109.129:9000/test2/Mutillinktable", "hdfs://192.168.109.129:9000/Mutillinktable"};
        logger.info("{组初始化的出纳和素=====>>>}" + otherArgs[0] + "======" + otherArgs[1]);
        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "MutilTableLink");
        job.setJarByClass(TableAloneLink.class);
        job.setMapperClass(MutilTableLinkMapper.class);
        job.setReducerClass(MutilTableLinkReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
        System.out.println("Finished");

    }
}
