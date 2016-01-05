package org.apache.kylin.client.script;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.kylin.common.util.DateFormat;

public class TestDate {
	public static void todayStartTime() {
		
	}
	public static void main(String[] args) throws Exception {
		Date today = new Date();
		SimpleDateFormat format = DateFormat.getDateFormat(DateFormat.DEFAULT_DATE_PATTERN);
		String cur = "2015-12-15 09:48:04";
		Date curDate = format.parse(cur);
		System.out.println(curDate);
//		System.out.println(date.getTime());
	}
}
