package pers.hwj.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author hwj
 * @Date 2020/8/6 7:52
 * @Desc: 计算两个数的最大值
 **/
public class returnMax extends UDF {
    public Double evaluate(Double a,Double b){
        if(a==null){
            a=0.0;
        }
        if(b==null)
            b=0.0;
        if(a>b){
            return a;
        }else{
            return b;
        }
    }
}

