# 第二周MR作业
## 一.核心代码逻辑
Mapper代码
```java
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
```
Reducer代码
```java
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
```
## 二.输出结果（部分截图）
![image](https://user-images.githubusercontent.com/16860476/158023547-69e4afa1-0535-4cde-8a9c-ad02e719ec60.png)
