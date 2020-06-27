/********************************************************************

PageRank_17341018_陈景宇_FinalProj_2020_6_27

*******************************************************************/
/********************************************************************

PageRank_17341018_陈景宇_FinalProj_2020_6_27

*******************************************************************/

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    // 自行设计阿尔法贝塔值
    private static float alpha = 0.85f; // alpha一般都是取0.85
    private static float bonus = 0.2f;
    
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        
        // idList: address list
        ArrayList<String> idList = new ArrayList<String>();
        // 输出
        String output = "  ";
        float pr = 0;

        // $: departure pagerank value
        // &: address
        for (Text id : values) {
            String idStr = id.toString();
            if (idStr.substring(0, 1).equals("$")) {
                pr += Float.parseFloat(idStr.substring(1));
            }
            else if (idStr.substring(0, 1).equals("&")) {
                String nextId = idStr.substring(1);
                idList.add(nextId);
            }
        }
        // pagerank 计算
        pr = pr * alpha + bonus;

        // 目标输出
        for (int i = 0; i < idList.size(); i++) {
            output = output + idList.get(i) + "  ";
        }
        
        // 目标字符串
        String result = pr + output;

        // 写入
        context.write(key, new Text(result));
    }
}