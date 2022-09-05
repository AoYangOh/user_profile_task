package com.say.userprofile.common.dao;

import com.say.userprofile.common.bean.TaskInfo;
import com.say.userprofile.common.utils.MySQLUtil;

public class TaskInfoDAO {

    public static TaskInfo getTaskInfo(String taskId ) {
        String sql= "select * from task_info ti where ti.id='"+taskId+"'";
        TaskInfo taskInfo  = MySQLUtil.queryOne(sql,TaskInfo.class, true);
        return taskInfo;

    }
}
