package com.say.userprofile.app;

import com.say.userprofile.common.bean.TagInfo;
import com.say.userprofile.common.constants.ConstCodes;
import com.say.userprofile.common.dao.TagInfoDAO;
import com.say.userprofile.common.utils.MyClickhouseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TaskBitmapApp {

    public static void main(String[] args) {

        //获得业务参数
        String taskId = args[0];
        String busiDate = args[1];

        //spark环境（任务4实质上是由java程序组合sql，交由clickhouse计算完成，这里使用spark上下文只是为了统一的提交方式，统一调度。）
        SparkConf conf = new SparkConf().setAppName("task_bitmap");//.setMaster("local[*]");
        SparkContext sparkContext=new SparkContext(conf);

        //1 查询已启用的标签列表
        List<TagInfo> tagInfoList = TagInfoDAO.getTagInfoListWithOn();


        //
        //2 因为要按标签值的类型不同，分成四份，分别插入4种bitmap表中
        List<TagInfo> tagInfoStringList = new ArrayList();
        List<TagInfo> tagInfoLongList = new ArrayList();
        List<TagInfo> tagInfoDecimalList = new ArrayList();
        List<TagInfo> tagInfoDateList = new ArrayList();

        //2.1分成四份
        for (TagInfo tagInfo : tagInfoList) {
            if (ConstCodes.TAG_VALUE_TYPE_STRING.equals(tagInfo.getTagValueType())) {
                tagInfoStringList.add(tagInfo);
            } else if (ConstCodes.TAG_VALUE_TYPE_LONG.equals(tagInfo.getTagValueType())) {
                tagInfoLongList.add(tagInfo);
            } else if (ConstCodes.TAG_VALUE_TYPE_DECIMAL.equals(tagInfo.getTagValueType())) {
                tagInfoDecimalList.add(tagInfo);
            } else if (ConstCodes.TAG_VALUE_TYPE_DATE.equals(tagInfo.getTagValueType())) {
                tagInfoDateList.add(tagInfo);
            }
        }

        //2.2 把4个list 转换为四条sql 分别执行
        insertBitmap(tagInfoStringList, "user_tag_value_string", busiDate);
        insertBitmap(tagInfoLongList, "user_tag_value_long", busiDate);
        insertBitmap(tagInfoDecimalList, "user_tag_value_decimal", busiDate);
        insertBitmap(tagInfoDateList, "user_tag_value_date", busiDate);


    }


    //跟list插入到目标bitmap表
    private static void insertBitmap(List<TagInfo> tagInfoList, String targetTableName, String busiDate) {

        if (tagInfoList.size() > 0) {
            List<String> tagCodeList = tagInfoList.stream()
                    .map(tagInfo ->
                            "('" + tagInfo.getTagCode().toLowerCase() + "'," + tagInfo.getTagCode().toLowerCase() + ")")
                    .collect(Collectors.toList());
            String tagCodeSQL = StringUtils.join(tagCodeList, ",");

            //幂等性处理
            String deleteSQL = " alter table " + targetTableName + " delete where dt='" + busiDate + "'";

            MyClickhouseUtil.executeSql(deleteSQL);

            String sourceTableName = "up_tag_merge_" + busiDate.replace("-", "");

            String sql =
                    " insert into  " + targetTableName +
                            "   select  tt.tg.1 as tag_code,if(tt.tg.2='','0',tt.tg.2) as tag_value , " +
                            " groupBitmapState(cast (uid as UInt64)  ) ,'" + busiDate + "'" +
                            " from ( " +
                            " select uid ,arrayJoin([" + tagCodeSQL + "]) tg " +
                            " from " + sourceTableName +
                            " ) tt" +
                            "  group by  tt.tg.1 as tag_code,tt.tg.2 ";


            System.out.println(sql);

            MyClickhouseUtil.executeSql(sql);
        }

    }

}

