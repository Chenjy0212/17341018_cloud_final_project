/********************************************************************

PageRank_17341018_陈景宇_FinalProj_2020_6_27

*******************************************************************/
/********************************************************************

起始节点的 pr 值对⽬标节点 pr 值进行贡献，并不停进⾏迭代。PageRank值通常需要迭代30-40次才能达到收敛。
数据格式为“起始节点 ID + PageRank值 + SPACE + ⽬标节点1 ID + 目标节点2 ID + 目标节点3 ID……”。

*******************************************************************/

import java.net.URI;
import java.util.Set;
import java.util.HashSet;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import java.util.StringTokenizer;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        // 输入的文件路径：  是NodeConvert数据格式转化的结果
        String pathIn = "/dataconvert";
        // 定义输出文件名
		String output = "/dataoutput";
		
        // 输出的文件路径：  这就是我们最后查看的，也是最重要的结果
        String pathOut = "/dataoutput";

        // IP源
        // 这里要和$HADOOP_HOME/etc/hadoop/core-site.xml文件中的
        // fs.defaultFS	NameNode URI	hdfs://host:port/ 对应好
        FileSystem.setDefaultUri(conf, new URI("hdfs://localhost:9000"));

        // 第一次输出文件的结果，以为后面需要进行迭代多次处理，所以记录序号方便查看
        pathOut = output + "1";

        // 多次迭代，也是pagerank mapreduce的和兴处理部分
        // 其中的迭代次数可以是正无穷，但是到达一定的次数时候，就可以看出收敛的结果
        // PageRank值通常需要迭代30-40次才能达到收敛
        // 为了减少不必要的操作量，这里就执行了30次迭代最后再查看结果
        for (int i = 1; i < 30; i++) {
            Job job = new Job(conf, "MapReduce pagerank");
            job.setJarByClass(PageRank.class);
            
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            
            // output map类与reduce类相同
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(pathIn));
            FileOutputFormat.setOutputPath(job, new Path(pathOut));
    
            // 新一轮的输入为上一轮的输出文件，这是迭代的典型代表
            pathIn = pathOut;
            pathOut = output + (i + 1);
            
            // 等待执行完成
            job.waitForCompletion(true);
        }
    }
}