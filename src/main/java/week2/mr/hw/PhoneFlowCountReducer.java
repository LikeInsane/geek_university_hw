package week2.mr.hw;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneFlowCountReducer extends Reducer<Text, PhoneFlowBean, Text, PhoneFlowBean> {

    private PhoneFlowBean outvalue = new PhoneFlowBean();

    /**
     * @param key     : 读取的一组数据的key
     * @param values  : 读取的一组数据的所有value
     * @param context : 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<PhoneFlowBean> values, Context context) throws IOException, InterruptedException {
        //1.定义totalUp，totalDown变量用于统计上下流量
        long totalUp = 0;
        long totalDown = 0;
        //2.遍历所有的value并对value进行累加
        for (PhoneFlowBean value : values) {
            //对value进行累加
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }
        //3.封装KV，并求总流量
        outvalue.setUpFlow(totalUp);
        outvalue.setDownFlow(totalDown);
        outvalue.setTotalFlow(totalUp+totalDown);

        //4.写出数据
        context.write(key, outvalue);
    }
}
