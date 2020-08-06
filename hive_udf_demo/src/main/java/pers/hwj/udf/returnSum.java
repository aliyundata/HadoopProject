package pers.hwj.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author hwj
 * @Date 2020/8/6 11:07
 * @Desc: 获得两数字的和
 **/
public class returnSum extends UDF {
    public Double SumTwo(Double a,Double b){
        if(a==null){
            a=0.0;
        }
        if(b==null)
            b=0.0;
        return a+b;
    }
}
