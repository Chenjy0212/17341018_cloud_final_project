/********************************************************************

NodeCovert_17341018_陈景宇_FinalProj_2020_6_27

*******************************************************************/
/********************************************************************

NodeConvertMapper 将 split 之后起始节点的目标节点们找到：
类中的私有变量 id 和 nextId 分别表示起始节点的 ID 和目标标节点的 ID
起始节点的 ID 作为 key，目标节点的 ID 作为 value 组成 key — value 对
map() 方法利用 Token 进行读数，目标节点之前加上 ‘$’ 符号，
便于区分起始节点和⽬标节点，写入 context。

*******************************************************************/

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class NodeConvertMapper extends Mapper<Object, Text, IntWritable, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        IntWritable id;
        IntWritable nextId;

        // 标记字符串
        StringTokenizer str = new StringTokenizer(value.toString());

        // 读入下一个标记
        if(str.hasMoreTokens()) {
            id = new IntWritable(Integer.parseInt(str.nextToken()));
        } else{
            return;
        }

        // &: address
        nextId = new IntWritable(Integer.parseInt(str.nextToken()));

        // 写入文档
        context.write(id, new Text("&" + nextId));

    }

}