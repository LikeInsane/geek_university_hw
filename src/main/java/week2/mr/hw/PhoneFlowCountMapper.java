package week2.mr.hw;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneFlowCountMapper extends Mapper<LongWritable, Text, Text, PhoneFlowBean> {

    private Text outkey = new Text();
    private PhoneFlowBean outvalue = new PhoneFlowBean();

    /**
     * @param key     读取的数据的偏移量
     * @param value   读取文件中的数据
     * @param context 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.将读取的数据类型转换为String
        String line = value.toString();
        //2.切割数据
        String[] lineSplit = line.split("\t");
        //3.封装Mapper阶段输出的K,V
        outkey.set(lineSplit[1]);
        outvalue.setUpFlow(Long.parseLong(lineSplit[8]));
        outvalue.setDownFlow(Long.parseLong(lineSplit[9]));
        //4.将K,V输出
        context.write(outkey, outvalue);
    }
}

