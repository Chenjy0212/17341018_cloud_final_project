/********************************************************************

NodeCovert_17341018_陈景宇_FinalProj_2020_6_27

*******************************************************************/
/********************************************************************

把 shuffle 之后的结果聚合到一起：
私有成员 idList 采用 ArrayList 的存储结构
内存 String 类型的目标节点 ID；reduce() 方法把 Mapper中 text 的数据转化成 String 形式
按照标示符，数据被整理成——“起始节点 ID + PageRank值 + SPACE + ⽬标节点1 ID + 目标节点2 ID + 目标节点3 ID……”格式
迭代开始的时候，每个节点默认的 PageRank 值设为1。

*******************************************************************/

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NodeConvertReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // idList: address list
        ArrayList<String> idList = new ArrayList<String>();
        
        // idString: address
        for(Text id : values) {
            String idStr = id.toString();
            if (idStr.substring(0,1).equals("&")) {
                idList.add(idStr.substring(1));
            }
        }
        // departure pagerank
        float pr = 1.0f;
        // result string
        String result = String.valueOf(pr) + " "; 
        for(int i = 0; i< idList.size();i++){
            result += idList.get(i) + " ";
        }

        // 写入
        context.write(key, new Text(result));
    }

}