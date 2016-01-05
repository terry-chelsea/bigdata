package com.terry.netease.calcite.test.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexTest {
	public static void main(String[] args) {
        String msg = "This is standby RM. Redirecting to the current active RM: http://hadoop54.photo.163.org:8088/ws/v1/cluster/apps/00000000";
        Pattern pt = Pattern.compile("This is standby RM\\. Redirecting to the current active RM\\:\\s+(.*?)");
        Matcher mtc = pt.matcher(msg);
        System.out.println(mtc.matches());
        System.out.println(mtc.group(mtc.groupCount()));
	}
}
