package pers.hwj;



import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author hwj
 * @Date 2020/8/7 16:26
 * @Desc: 将业务逻辑相关的信息：哪个是 Mapper,哪个是 reducer,要处理的数据在哪，输出的数据放哪) 描述成一个 job对象
 *
 **/
public class WordCountMain {
    // 将这个描述好的对象提交给集群去运行
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //  Configuration 封装了对应客户端或服务器的配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountMain.class);
        // 指定Map阶段的处理方式
        job.setMapperClass(WordCountMapper.class);
        // 指定 reduce 阶段的处理方式
        job.setReducerClass(WordCountReducer.class);
        // 指定 Map 阶段键值对输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 指定 reduce 阶段输出到文件的键值对类型

//        FileInputFormat.setInputPaths(job,new Path("file:///G:\\input"));
        FileInputFormat.setInputPaths(job,"hdfs://node01:8020/wordcount");
        FileOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/wordcount/output"));
        // 向 yarn 集群提交这个 job
        boolean res=job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
