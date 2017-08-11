package org.exer2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TestNewCombinerGrouping;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;


public class Temperature {

    private static Logger logger = Logger.getLogger(Temperature.class);

    /**
     * 四个泛型类型分别代表：
     * <p>
     * KeyIn        Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）
     * <p>
     * ValueIn      Mapper的输入数据的Value，这里是每行文字
     * <p>
     * KeyOut       Mapper的输出数据的Key，这里是每行文字中的“年份”
     * <p>
     * ValueOut     Mapper的输出数据的Value，这里是每行文字中的“气温”
     */

    static class TempMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 打印样本: Before Mapper: 0, 2000010115
            logger.info("{map读取到的数据为:}=====>{key====>}"+key+"{value====>}"+value);
            String line = value.toString();
            logger.info("{map拿到数据的value为}====>"+line);
            String year = line.substring(0, 4);
            logger.info("{提取的数据为}=====>"+year);
            String  temperature1 = line.substring(4,line.length()).toString();
            logger.info("{the temperaturle 的结果是}"+temperature1);
            String[] strings = temperature1.split("");
            logger.info("{分离之后的数组的长度为====>}"+strings.length);
            for(int i=0;i<strings.length;i++){
                logger.info("{便利开始=====>}"+i);
                System.out.println(strings[i]);
                logger.info("{String的结果为====>}"+strings[i]);
            }
            Text text = new Text(year);
            logger.info("{map的key为====>}"+text.toString());
            for(int i=0;i<=5;i++){
                logger.info("{for循环的第}===>"+i+1+"循环");
                logger.info("{获取到值}====>"+strings[i+1]);
                Text text1 = new Text(strings[i+1]);
                context.write(text,text1);
                logger.info("{写入成功!!!}");
            }
//            //2000 010115
//            logger.info("{the temperature的内容为:}====>"+temperature);
//            context.write(new Text(year), new IntWritable(temperature));
//            // 打印样本: After Mapper:2000, 15
//            logger.info("{最终结果....}===>"+"======" + "After Mapper:" + new Text(year) + ", " + new IntWritable(temperature));
//            System.out.println("======" + "After Mapper:" + new Text(year) + ", " + new IntWritable(temperature));
        }

    }

    /**
     * 四个泛型类型分别代表：
     * <p>
     * KeyIn        Reducer的输入数据的Key，这里是每行文字中的“年份”
     * <p>
     * ValueIn      Reducer的输入数据的Value，这里是每行文字中的“气温”
     * <p>
     * KeyOut       Reducer的输出数据的Key，这里是不重复的“年份”
     * <p>
     * ValueOut     Reducer的输出数据的Value，这里是这一年中的“最高气温”
     */


    static class TempReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxValue = Integer.MIN_VALUE;
            StringBuffer sb = new StringBuffer();
            for(Text text:values){
                maxValue = Math.max(maxValue,Integer.valueOf(text.toString()));
                logger.info("{reduce的接收到的values为}===>"+maxValue);
                sb.append(text).append(", ");
                logger.info("{the 传过来的内容is }====>"+sb.toString());
            }
            //取values的最大值
//            for (IntWritable value : values) {
//                maxValue = Math.max(maxValue, value.get());
//                logger.info("{reduce的接收到的values为}===>"+maxValue);
//                sb.append(value).append(", ");
//                logger.info("{the 传过来的内容is }====>"+sb.toString());
//            }
            // 打印样本： Before Reduce: 2000, 15, 23, 99, 12, 22,
            System.out.print("Before Reduce: " + key + ", " + sb.toString());
            logger.info("{Before Reduce: }==========>" + key + ", " + sb.toString());
//            context.write(key, new IntWritable(maxValue));
            context.write(key,new Text(maxValue+""));
            // 打印样本： After Reduce: 2000, 99
            logger.info("{============}===>"+"======" + "After Reduce: " + key + ", " + maxValue);
            System.out.println("======" + "After Reduce: " + key + ", " + maxValue);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

//        String[] otherArgs = {"hdfs://192.168.109.129:9000/test1/weather.txt", "hdfs://192.168.109.129:9000/weather"};
        String[] otherArgs = {"hdfs://192.168.109.129:9000/test2/weather", "hdfs://192.168.109.129:9000/weather"};
        logger.info("{组初始化的出纳和素=====>>>}"+otherArgs[0]+"======"+otherArgs[1]);
        Configuration hadoopConfig = new Configuration();
        Job job = new Job(hadoopConfig);
        //指定自定义的Mapper和Reducer作为两个阶段的任务处理类
        job.setJarByClass(Temperature.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);
        //设置最后输出结果的Key和Value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job执行作业时输入和输出文件的路径
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //执行job，直到完成
        job.waitForCompletion(true);
        System.out.println("Finished");

    }

}
