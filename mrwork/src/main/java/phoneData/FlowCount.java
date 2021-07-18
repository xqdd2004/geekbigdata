package phoneData;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 统计每一个手机号耗费的总上行流量、下行流量、总流量
 * (1) 输入数据格式：
 * 时间戳、电话号码、基站的物理地址、访问网址的ip、网站域名、数据包、接包数、上行/传流量、下行/载流量、响应码
 * (2)最终输出的数据格式：
 * 手机号码		上行流量        下行流量		总流量
 *
 */
public class FlowCount {

    /**
     * 流量统计Mapper实现，取出每行的数据的手机号为key，以及数据上行流量和下行流量
     */
    public static class FlowCountMapper
            extends Mapper<Object, Text, Text, FlowBean>{

        private final static FlowBean phoneData = new FlowBean();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String lineData = value.toString();
            String arrLine[] = lineData.split("\t");
            String phoneNum = arrLine[1];

            long upFlow = Long.parseLong(arrLine[8]);
            long downFlow = Long.parseLong(arrLine[9]);
            phoneData.setUpFlow(upFlow);
            phoneData.setDownFlow(downFlow);
            phoneData.setSumFlow( upFlow + downFlow );

//            System.out.println("mapper\t" + phoneNum + "\t" + phoneData.toString());
            context.write( new Text(phoneNum), phoneData);
        }
    }


    /**
     * 手机流量统计Reducer实现
     */
    public static class FlowCountReducer
            extends Reducer<Text,FlowBean,Text,FlowBean> {
        private FlowBean result = new FlowBean();

        public void reduce(Text key, Iterable<FlowBean> values,
                           Context context
        ) throws IOException, InterruptedException {
            //需要初始化, 按手机号进行累计添加上行流量、下行流量和总流量
            result.setUpFlow(0l);
            result.setDownFlow(0l);
            result.setSumFlow(0l);
            for (FlowBean val : values) {
                System.out.println("reduce\t" + key.toString() + "\t" + val.toString());
                result.setUpFlow( result.getUpFlow() + val.getUpFlow());
                result.setDownFlow(result.getDownFlow() + val.getDownFlow());
                result.setSumFlow( result.getSumFlow() + val.getSumFlow());
            }
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Flow count");
        job.setJarByClass(FlowCount.class);
        job.setMapperClass(FlowCountMapper.class);
        job.setCombinerClass(FlowCountReducer.class);
        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop0:9000/user/student/chenjd/mrwork/input/HTTP_20130313143750.dat"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop0:9000/user/student/chenjd/mrwork/ouput7"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
