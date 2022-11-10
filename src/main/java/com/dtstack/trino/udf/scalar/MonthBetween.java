/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.trino.udf.scalar;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static io.airlift.slice.Slices.utf8Slice;

/**
 * @author chuixue
 * @create 2022-11-09 10:35
 * @description
 **/
public class MonthBetween {
    private static final Logger log = LoggerFactory.getLogger(MonthBetween.class);
    static final String[] possiblePatterns =
            {
                    "yyyy-MM-dd",
                    "yyyy-MM-dd HH:mm:ss",
                    "yyyyMMdd",
                    "yyyy/MM/dd",
                    "yyyy/MM/dd HH:mm:ss",
                    "yyyy年MM月dd日",
                    "yyyy MM dd"
            };

    public static void main(String[] args) {
        System.out.println(monthsBetween(utf8Slice("2022-10-10"), utf8Slice("2021-10-1")));
        System.out.println(monthsBetween(utf8Slice("20221010"), utf8Slice("2021101")));
        System.out.println(monthsBetween(utf8Slice("2022/10/10"), utf8Slice("2021/10/1")));
        System.out.println(monthsBetween(utf8Slice("2022-10-10 10:10:10"), utf8Slice("2021-10-1 10:10:10")));
        System.out.println(monthsBetween(utf8Slice("2022/10/10 10:10:10"), utf8Slice("2021/10/1 10:10:10")));
        System.out.println(monthsBetween(utf8Slice("10:10:10"), utf8Slice("1:10:10")));

        System.out.println(monthsBetween(utf8Slice("2020-10-10"), utf8Slice("2021-10-1")));
        System.out.println(monthsBetween(utf8Slice("20201010"), utf8Slice("2021101")));
        System.out.println(monthsBetween(utf8Slice("2020/10/10"), utf8Slice("2021/10/1")));
        System.out.println(monthsBetween(utf8Slice("2020-10-10 10:10:10"), utf8Slice("2021-10-1 10:10:10")));
        System.out.println(monthsBetween(utf8Slice("2020/10/10 10:10:10"), utf8Slice("2021/10/1 10:10:10")));
        System.out.println(monthsBetween(utf8Slice("10:10:10"), utf8Slice("1:10:10")));

        System.out.println(monthsBetween(utf8Slice("1667976132181"), utf8Slice("1667876132181")));

        System.out.println(monthsBetween(1602295810000000l, 1633054210000000l));

    }

    @SqlNullable
    @Description("months_between(Timestamp end, Timestamp start)")
    @ScalarFunction("months_between")
    @SqlType(StandardTypes.DOUBLE)
    public static Double monthsBetween(@SqlType("timestamp(3)") long end, @SqlType("timestamp(3)") long start) {
        String sEnd = new Timestamp(end/1000).toString();
        String sStart = new Timestamp(start/1000).toString();
        return getMonthsBetween(sEnd, sStart);
    }

    @SqlNullable
    @Description("months_between(String end, String start)")
    @ScalarFunction("months_between")
    @SqlType(StandardTypes.DOUBLE)
    public static Double monthsBetween(@SqlType(StandardTypes.VARCHAR) Slice end, @SqlType(StandardTypes.VARCHAR) Slice start) {
        String sEnd = end.toStringUtf8();
        String sStart = start.toStringUtf8();
        return getMonthsBetween(sEnd, sStart);
    }

    private static Double getMonthsBetween(String sEnd, String sStart) {
        try {
            Date startTime = ParserDate(sEnd);
            Date endTime = ParserDate(sStart);
            if (startTime == null || endTime == null) {
                log.warn("'{}' or '{}' can not parse.", sEnd, sStart);
                return null;
            }

            Calendar startCalendar = Calendar.getInstance();
            startCalendar.setTime(startTime);
            Calendar endtCalendar = Calendar.getInstance();
            endtCalendar.setTime(endTime);

            int startYear = startCalendar.get(Calendar.YEAR);
            int endYear = endtCalendar.get(Calendar.YEAR);
            int startMonth = startCalendar.get(Calendar.MONTH);
            int endMonth = endtCalendar.get(Calendar.MONTH);
            int startDay = startCalendar.get(Calendar.DATE);
            int endDay = endtCalendar.get(Calendar.DATE);
            int startHour = startCalendar.get(Calendar.HOUR_OF_DAY);
            int startMinute = startCalendar.get(Calendar.MINUTE);
            int startSecond = startCalendar.get(Calendar.SECOND);
            int endHour = endtCalendar.get(Calendar.HOUR_OF_DAY);
            int endMinute = endtCalendar.get(Calendar.MINUTE);
            int endSecond = endtCalendar.get(Calendar.SECOND);

            double result = (startYear * 12 + startMonth) - (endYear * 12 + endMonth);
            int countDay = ((startDay * 24) - (endDay * 24)) / 24;

            double countHourDay = (startHour - endHour) / 24d;
            double countMinuteDay = (startMinute - endMinute) / (60d * 24d);
            double countSecondDay = (startSecond - endSecond) / (60d * 24d * 60d);
            double factor = (1d / 31d);
            result += countDay * factor;
            result += countHourDay * factor;
            result += countMinuteDay * factor;
            result += countSecondDay * factor;
            return result;
        } catch (Exception e) {
            log.error(e.toString());
        }
        return null;
    }

    private static Date ParserDate(String timeStr) {
        SimpleDateFormat df = new SimpleDateFormat();
        Date date;
        for (String pattern : possiblePatterns) {
            df.applyPattern(pattern);
            df.setLenient(false);//设置解析日期格式是否严格解析日期
            ParsePosition pos = new ParsePosition(0);
            date = df.parse(timeStr, pos);
            if (date != null) {
                return date;
            }
        }
        return null;
    }
}
