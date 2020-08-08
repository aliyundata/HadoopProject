package pers.hwj;

import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author hwj
 * @Date 2020/8/7 12:54
 * @Desc: 将 k1 v1 转换为 k2 v2
 **/
/*
k1 行偏移量   LongWritable
v1 行文本     Text
k2 单词文本   Text
v2 个数       LongWritable
 */
/*
步骤：重写提供的map方法
1. 拆开文本数据
2. 获取 k2,v2
3. 将 k2,v2 写入上下文中
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        LongWritable longWritable = new LongWritable();
        // 拿到一行数据转换为 String
        // 将这一行切分成各个单词
        // 遍历数组，输出 k2,v2，写入到上下文中
        String line = value.toString();
        String[] words = line.split(" ");
        for(String word: words){
            text.set(word);
            longWritable.set(1);
            context.write(text,longWritable);
        }
    }
}
