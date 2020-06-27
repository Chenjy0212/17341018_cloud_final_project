/********************************************************************

PageRank_17341018_陈景宇_FinalProj_2020_6_27

*******************************************************************/
/********************************************************************

对数据进行了平均加权：
私有成员 id 表示起始节点的 ID、pr 表示⽬标节点 ID、count 给目标节点记数、average_pr 计算平均贡献
起始节点的 ID 作为 key，其权值和目标节点的 ID 通过不同的符号标示作为 value，组成 key — value 对
map() 方法首先对起始节点的目标节点个数进行了记数；然后用起始节点的 PageRank 值除以目标节点个数，
得到了平均值——起始节点对每个目标节点 PageRank 值的平均贡献；最后，用 ‘$’ 标示对目标节点们的
平均 PageRank 贡献、用 ‘&’ 标示目标节点们。

*******************************************************************/

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Object, Text, IntWritable, Text> {
    private IntWritable id;
    private String nextId;
    private int count;
    private float averagePr;
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        // 标记字符串
        StringTokenizer str = new StringTokenizer(value.toString());
        
        // 读入下一个标记字符串
        if (str.hasMoreTokens()) {
            id = new IntWritable(Integer.parseInt(str.nextToken()));
        } else {
            return;
        }

        nextId = str.nextToken();
        
        // 获取标记字符串的计数
        count = str.countTokens();

        // 各标记字符串平均奖励
        averagePr = Float.parseFloat(nextId) / count;
        
        // $: departure pagerank value
        // &: address
        while (str.hasMoreTokens()) {

            String nextId = str.nextToken();
            
            Text tmpText = new Text("$" + averagePr);
            context.write(new IntWritable(Integer.parseInt(nextId)), tmpText);

            tmpText = new Text("&" + nextId);
            context.write(id, tmpText);
        }
    }
}