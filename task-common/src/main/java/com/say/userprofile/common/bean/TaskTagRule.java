package com.say.userprofile.common.bean;

import lombok.Data;

@Data
public class TaskTagRule {

    Long id;

    Long tagId;

    Long taskId;

    String queryValue;

    Long subTagId;

    String subTagValue;

}

