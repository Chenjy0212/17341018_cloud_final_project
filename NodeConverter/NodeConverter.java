/********************************************************************

NodeCovert_17341018_陈景宇_FinalProj_2020_6_27

*******************************************************************/
/********************************************************************

这部分代码主要是进行数据处理，进行格式上的转化，以方便后续的迭代工作。

数据格式被转化成：“起始节点 ID + PageRank值 + SPACE + 目标节点1 ID + 目标节点2 ID + 目标节点3 ID……”。

*******************************************************************/

import java.net.URI;
import java.util.Set;
import java.util.HashSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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

public class NodeConverter {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        // 输入的文件路径，也就是soc-Epinions1.txt的内容
        String pathIn = "/dataset";
        // 输出文件路径，作为后面pagerank的输入文件内容
        String pathOut = "/dataconvert";

        // IP源
        // 这里要和$HADOOP_HOME/etc/hadoop/core-site.xml文件中的
        // fs.defaultFS	NameNode URI	hdfs://host:port/ 对应好
        FileSystem.setDefaultUri(conf, new URI("hdfs://localhost:9000"));

        // 数据预处理
        Job job = new Job(conf, "MapReduce pretreatment");
        job.setJarByClass(NodeConverter.class);
        
        job.setMapperClass(NodeConvertMapper.class);
        job.setReducerClass(NodeConvertReducer.class);
        
        // output map类与reduce类相同
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));

        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}