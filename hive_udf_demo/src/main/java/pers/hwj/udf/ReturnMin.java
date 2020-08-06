package pers.hwj.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author hwj
 * @Date 2020/8/6 11:23
 * @Desc:
 **/
public class ReturnMin extends UDF {
    public Double calculate(Double a,Double b){
        if(a==null){
            a=0.0;
        }
        if(b==null)
            b=0.0;
        if(a>b){
            return b;
        }else{
            return a;
        }
    }
}
