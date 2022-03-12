package week2.mr.hw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PhoneFlowCountDriver {

    public static void main(String[] args) throws Exception {
        //1.创建job对象
        Job job = Job.getInstance(new Configuration());
        //2.设置Jar加载的路径
        job.setJarByClass(PhoneFlowCountDriver.class);
        //3.设置Mapper和Reducer
        job.setMapperClass(PhoneFlowCountMapper.class);
        job.setReducerClass(PhoneFlowCountReducer.class);
        //4.设置mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneFlowBean.class);
        //5.设置最终输出的类型（在这里是Reducer输出的k,v的类型）
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneFlowBean.class);
        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7.执行job
        /*  boolean waitForCompletion(boolean verbose)
            verbose : 是否打印进度
            返回值 ：如果为true表示job执行成功
         */
        job.waitForCompletion(true);
    }
}
