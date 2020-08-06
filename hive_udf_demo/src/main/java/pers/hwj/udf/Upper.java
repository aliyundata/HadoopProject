package pers.hwj.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @Author hwj
 * @Date 2020/8/5 20:15
 * @Desc: 模拟hive的upper方法：将字符串的第一个字母转大写，其它不变
 **/
public class Upper extends UDF {
    public Text evaluate(final Text line){
        if(line.toString()!=null&& !line.toString().equals("")){
            String str=line.toString().substring(0,1).toUpperCase()+line.toString().substring(1);
            return new Text(str);

        }
        return new Text("");
    }
}
