package pers.hwj;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author hwj
 * @Date 2020/8/7 16:11
 * @Desc: 将 k2 v2 转化为 k3,v3
 **/
/*
k2 单词文本            Text
v2 <迭代器> 个数       LongWritable    <1,1,1,1>
k3 单词文本             Text
v3 每个单词累积个数  LongWritable       4
 */
public class WordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    /*
步骤：重写提供的 reduce 方法
1. 拆开文本数据
2. 获取 k2,v2
3. 将 k2,v2 写入上下文中
 */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count=0;
        for(LongWritable value:values){
            count=count+value.get();
        }
        context.write(key,new LongWritable(count));
    }
}
