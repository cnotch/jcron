package net.tomatox.jcron;

import org.junit.Test;

import java.io.*;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.*;

public class CronExpressionTest {
    private static final DateTimeFormatter secondFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter weekdayFormatter;

    static {
        Map<Long, String> dow = new HashMap<>();
        dow.put(1L, "Mon");
        dow.put(2L, "Tue");
        dow.put(3L, "Wed");
        dow.put(4L, "Thu");
        dow.put(5L, "Fri");
        dow.put(6L, "Sat");
        dow.put(7L, "Sun");
        weekdayFormatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .parseLenient()
                .optionalStart()
                .appendText(ChronoField.DAY_OF_WEEK, dow)
                .appendLiteral(" ")
                .optionalEnd()
                .appendPattern("yyyy-MM-dd HH:mm")
                .toFormatter();
    }
    @Test
    public void testScheduleOfCron() {
        try {
            CronExpression expr = CronExpression.parse("0-59 * * * * ?");
            ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(4);


            final long start = System.currentTimeMillis();
            ScheduledFuture<?> future = expr.schedule(pool, ()->System.out.println(System.currentTimeMillis() - start));

            Thread.sleep(10005);
            future.cancel(false);
            Thread.sleep(1005);
            pool.shutdown();

        } catch (InterruptedException | ParseException e) {
            e.printStackTrace();
        }
    }

    @Test//(expected = ParseException.class)
    public void testParseException() {
        String[] cases = {
                "60 * * * * * *", "'60'",
                "* 61 * * * * *", "'61'",
                "* * 24 * * * *", "'24'",
                "* * * 32 * * *", "'32'",
                "* * * * 13 * *", "'13'",
                "* * * * * 8 *", "'8'",
                "* * * * * * 1969", "'1969'",
                "* * * * * * 2010-2001", "'2010-2001'",
        };
        for (int i = 0; i < cases.length; i += 2) {
            try {
                CronExpression expr = CronExpression.parse(cases[i]);
                assertNull(cases[i], expr);
            } catch (ParseException pe) {
                assertTrue(cases[i], pe.getMessage().endsWith(cases[i + 1]));
            }
        }
    }

    @Test
    public void test() {
        testExpression("每天上午10点，下午2点，4点", "0 0 10,14,16 * * ?");
        testExpression("朝九晚五工作时间内每半小时", "0 0/30 9-17 * * ?");
        testExpression("表示每个星期三中午12点", "0 0 12 ? * WED");
        testExpression("表示每个星期六-星期一中午12点", "0 0 12 ? * sat-mon");

    }

    public void testExpression(String name, String expression) {
        try {
            System.out.println(name);
            System.out.println(expression);

            CronExpression cron = CronExpression.parse(expression);
            LocalDateTime now = LocalDateTime.now();
            System.out.println("开始时间：" + now);
            for (int i = 0; i < 10; i++) {
                now = cron.next(now);
                System.out.println(now.format(weekdayFormatter));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPerformance() {
        testPerformance("每天上午10点，下午2点，4点", "0 0 10,14,16 * * ?", 10000);
        testPerformance("朝九晚五工作时间内每半小时", "0 0/30 9-17 * * ?", 10000);
        testPerformance("表示每个星期三中午12点", "0 0 12 ? * WED", 5000);
    }

    public void testPerformance(String name, String expression, int count) {
        try {
            System.out.println(name + "(执行 " + count + " 次 ): " + expression);
            long start = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                CronExpression expr = CronExpression.parse(expression);
                assertNotNull(expr);
            }
            System.out.println("Parse 耗时：" + (System.currentTimeMillis() - start));

            CronExpression expr = CronExpression.parse(expression);
            Date next = Calendar.getInstance().getTime();
            start = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                next = expr.next(next);
                assertNotNull(next);
            }
            System.out.println("Next 耗时：" + (System.currentTimeMillis() - start));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSerialization() {
        for (TestCase testCase : testCases) {
            for (int i = 0; i < testCase.data.length; i += 2) {
                try {
                    CronExpression srcExpr = CronExpression.parse(testCase.cron);
                    CronExpression expr;
                    {
                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        ObjectOutputStream ooS;
                        ooS = new ObjectOutputStream(byteArrayOutputStream);
                        ooS.writeObject(srcExpr);

                        byte[] data = byteArrayOutputStream.toByteArray();
                        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
                                data);
                        ObjectInputStream oiStream = new ObjectInputStream(byteArrayInputStream);
                        expr = (CronExpression) oiStream
                                .readObject();
                    }
                    LocalDateTime input = LocalDateTime.parse(testCase.data[i], secondFormatter);
                    LocalDateTime output;
                    output = expr.next(input);
                    assertEquals(expr.getExpression(), testCase.data[i + 1], output.format(testCase.formatter));
                } catch (ParseException | ClassNotFoundException | IOException e) {
                    e.printStackTrace();
                    assertNull(e.getMessage(), e);
                }
            }
        }
    }

    @Test
    public void testClone() {
        for (TestCase testCase : testCases) {
            for (int i = 0; i < testCase.data.length; i += 2) {
                try {
                    CronExpression srcExpr = CronExpression.parse(testCase.cron);
                    CronExpression expr = srcExpr.clone();

                    LocalDateTime input = LocalDateTime.parse(testCase.data[i], secondFormatter);
                    LocalDateTime output;
                    output = expr.next(input);
                    assertEquals(expr.getExpression(), testCase.data[i + 1], output.format(testCase.formatter));
                } catch (ParseException pe) {
                    pe.printStackTrace();
                    assertNull(pe.getMessage(), pe);
                }
            }
        }
    }

    @Test
    public void testNextLocal() {
        for (TestCase testCase : testCases) {
            for (int i = 0; i < testCase.data.length; i += 2) {
                try {
                    CronExpression expr = CronExpression.parse(testCase.cron);
                    LocalDateTime input = LocalDateTime.parse(testCase.data[i], secondFormatter);
                    LocalDateTime output;
                    output = expr.next(input);
                    assertEquals(expr.getExpression(), testCase.data[i + 1], output.format(testCase.formatter));
                } catch (ParseException pe) {
                    pe.printStackTrace();
                    assertNull(pe.getMessage(), pe);
                }
            }
        }
    }

    @Test
    public void testNextZoned() {
        for (TestCase testCase : testCases) {
            for (int i = 0; i < testCase.data.length; i += 2) {
                try {
                    CronExpression expr = CronExpression.parse(testCase.cron);
                    LocalDateTime input = LocalDateTime.parse(testCase.data[i], secondFormatter);
                    ZonedDateTime zoneInput = ZonedDateTime.of(input, ZoneId.systemDefault());
                    ZonedDateTime output;
                    output = expr.next(zoneInput);
                    assertEquals(expr.getExpression(), testCase.data[i + 1], output.format(testCase.formatter));
                } catch (ParseException pe) {
                    pe.printStackTrace();
                    assertNull(pe.getMessage(), pe);
                }
            }
        }
    }

    private static final TestCase[] testCases = {
// Seconds
            new TestCase(
                    "* * * * * * *",
                    secondFormatter,
                    new String[]{
                            "2013-01-01 00:00:00", "2013-01-01 00:00:01",
                            "2013-01-01 00:00:59", "2013-01-01 00:01:00",
                            "2013-01-01 00:59:59", "2013-01-01 01:00:00",
                            "2013-01-01 23:59:59", "2013-01-02 00:00:00",
                            "2013-02-28 23:59:59", "2013-03-01 00:00:00",
                            "2016-02-28 23:59:59", "2016-02-29 00:00:00",
                            "2012-12-31 23:59:59", "2013-01-01 00:00:00"
                    }
            ),
// every 5 Second
            new TestCase(
                    "*/5 * * * * * *",
                    secondFormatter,
                    new String[]{
                            "2013-01-01 00:00:00", "2013-01-01 00:00:05",
                            "2013-01-01 00:00:59", "2013-01-01 00:01:00",
                            "2013-01-01 00:59:59", "2013-01-01 01:00:00",
                            "2013-01-01 23:59:59", "2013-01-02 00:00:00",
                            "2013-02-28 23:59:59", "2013-03-01 00:00:00",
                            "2016-02-28 23:59:59", "2016-02-29 00:00:00",
                            "2012-12-31 23:59:59", "2013-01-01 00:00:00"
                    }
            ),
// Minutes
            new TestCase(
                    "* * * * *",
                    secondFormatter,
                    new String[]{
                            "2013-01-01 00:00:00", "2013-01-01 00:01:00",
                            "2013-01-01 00:00:59", "2013-01-01 00:01:00",
                            "2013-01-01 00:59:00", "2013-01-01 01:00:00",
                            "2013-01-01 23:59:00", "2013-01-02 00:00:00",
                            "2013-02-28 23:59:00", "2013-03-01 00:00:00",
                            "2016-02-28 23:59:00", "2016-02-29 00:00:00",
                            "2012-12-31 23:59:00", "2013-01-01 00:00:00"
                    }
            ),
// Minutes with interval
            new TestCase(
                    "17-43/5 * * * *",
                    secondFormatter,
                    new String[]{
                            "2013-01-01 00:00:00", "2013-01-01 00:17:00",
                            "2013-01-01 00:16:59", "2013-01-01 00:17:00",
                            "2013-01-01 00:30:00", "2013-01-01 00:32:00",
                            "2013-01-01 00:50:00", "2013-01-01 01:17:00",
                            "2013-01-01 23:50:00", "2013-01-02 00:17:00",
                            "2013-02-28 23:50:00", "2013-03-01 00:17:00",
                            "2016-02-28 23:50:00", "2016-02-29 00:17:00",
                            "2012-12-31 23:50:00", "2013-01-01 00:17:00"
                    }
            ),
// Minutes interval, list
            new TestCase(
                    "15-30/4,55 * * * *",
                    secondFormatter,
                    new String[]{
                            "2013-01-01 00:00:00", "2013-01-01 00:15:00",
                            "2013-01-01 00:16:00", "2013-01-01 00:19:00",
                            "2013-01-01 00:30:00", "2013-01-01 00:55:00",
                            "2013-01-01 00:55:00", "2013-01-01 01:15:00",
                            "2013-01-01 23:55:00", "2013-01-02 00:15:00",
                            "2013-02-28 23:55:00", "2013-03-01 00:15:00",
                            "2016-02-28 23:55:00", "2016-02-29 00:15:00",
                            "2012-12-31 23:54:00", "2012-12-31 23:55:00",
                            "2012-12-31 23:55:00", "2013-01-01 00:15:00"
                    }
            ),
// Days of week
            new TestCase(
                    "0 0 * * MON",
                    weekdayFormatter,
                    new String[]{
//                            "2013-01-01 00:00:00", "Mon 2013-01-07 00:00",
                            "2013-01-28 00:00:00", "Mon 2013-02-04 00:00",
                            "2013-12-30 00:30:00", "Mon 2014-01-06 00:00"
                    }
            ),
            new TestCase(
                    "0 0 * * friday",
                    weekdayFormatter,
                    new String[]{
                            "2013-01-01 00:00:00", "Fri 2013-01-04 00:00",
                            "2013-01-28 00:00:00", "Fri 2013-02-01 00:00",
                            "2013-12-30 00:30:00", "Fri 2014-01-03 00:00"
                    }
            ),
            new TestCase(
                    "0 0 * * 6,7",
                    weekdayFormatter,
                    new String[]{
                            "2013-01-01 00:00:00", "Sat 2013-01-05 00:00",
                            "2013-01-28 00:00:00", "Sat 2013-02-02 00:00",
                            "2013-12-30 00:30:00", "Sat 2014-01-04 00:00"
                    }
            ),
// Specific days of week
            new TestCase(
                    "0 0 * * 6#5",
                    weekdayFormatter,
                    new String[]{
                            "2013-09-02 00:00:00", "Sat 2013-11-30 00:00"
                    }
            ),
// Work day of month
            new TestCase(
                    "0 0 14W * *",
                    weekdayFormatter,
                    new String[]{
                            "2013-03-31 00:00:00", "Mon 2013-04-15 00:00",
                            "2013-08-31 00:00:00", "Fri 2013-09-13 00:00"
                    }
            ),
// Work day of month -- end of month
            new TestCase(
                    "0 0 30W * *",
                    weekdayFormatter,
                    new String[]{
                            "2013-03-02 00:00:00", "Fri 2013-03-29 00:00",
                            "2013-06-02 00:00:00", "Fri 2013-06-28 00:00",
                            "2013-09-02 00:00:00", "Mon 2013-09-30 00:00",
                            "2013-11-02 00:00:00", "Fri 2013-11-29 00:00"
                    }
            ),
// Last day of month
            new TestCase(
                    "0 0 L * *",
                    weekdayFormatter,
                    new String[]{
                            "2013-09-02 00:00:00", "Mon 2013-09-30 00:00",
                            "2014-01-01 00:00:00", "Fri 2014-01-31 00:00",
                            "2014-02-01 00:00:00", "Fri 2014-02-28 00:00",
                            "2016-02-15 00:00:00", "Mon 2016-02-29 00:00"
                    }
            ),
// Last work day of month
            new TestCase(
                    "0 0 LW * *",
                    weekdayFormatter,
                    new String[]{
                            "2013-09-02 00:00:00", "Mon 2013-09-30 00:00",
                            "2013-11-02 00:00:00", "Fri 2013-11-29 00:00",
                            "2014-08-15 00:00:00", "Fri 2014-08-29 00:00"
                    }
            ),

            new TestCase(
                    "0 30 08 15 Jul ?",
                    weekdayFormatter,
                    new String[]{
                            "2012-07-16 08:29:59", "Mon 2013-07-15 08:30"
                    }
            ),

            new TestCase(
                    "0 * * */10 * Sun",
                    weekdayFormatter,
                    new String[]{
                            "2012-07-14 23:59:59", "Sun 2012-07-15 00:00"
                    }
            ),

            new TestCase(
                    "0 * * * 7 Sun 2020",
                    weekdayFormatter,
                    new String[]{
                            "2012-07-14 23:59:59", "Sun 2020-07-05 00:00"
                    }
            )
            ,

            new TestCase(
                    "0 0 12 ? * sat-mon",
                    weekdayFormatter,
                    new String[]{
                            "2022-08-26 07:52:05", "Sat 2022-08-27 12:00",
                            "2022-08-27 12:00:05", "Sun 2022-08-28 12:00",
                            "2022-08-28 12:00:05", "Mon 2022-08-29 12:00",
                            "2022-08-29 12:00:05", "Sat 2022-09-03 12:00"
                    }
            )
            // TODO: more tests
    };

    private static class TestCase {
        String cron;
        DateTimeFormatter formatter;
        String[] data;

        public TestCase(String cron, DateTimeFormatter formatter, String[] data) {
            this.cron = cron;
            this.formatter = formatter;
            this.data = data;
        }
    }
}
