package com.say.userprofile.common.dao;

import com.say.userprofile.common.bean.TaskTagRule;
import com.say.userprofile.common.utils.MySQLUtil;

import java.util.List;

public class TaskTagRuleDAO {

    public static List<TaskTagRule> getTaskTagRuleList(String taskId) {

        String sql = "select tr.*, tag_name as sub_tag_value " +
                " from task_tag_rule tr " +
                " join tag_info ti on sub_tag_id=ti.id " +
                "where  task_id='" + taskId + "'";
        List<TaskTagRule> taskTagRules =
                MySQLUtil.queryList(sql, TaskTagRule.class, true);
        return taskTagRules;

    }
}

