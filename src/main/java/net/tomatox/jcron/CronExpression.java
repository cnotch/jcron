// Copyright (c) 2018,CAOHONGJU All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package net.tomatox.jcron;

import java.io.Serializable;
import java.text.ParseException;
import java.time.*;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static net.tomatox.jcron.CronExpression.Field.*;

/**
 * Provides a parser and evaluator for unix-like cron expressions. Cron
 * expressions provide the ability to specify complex time combinations such as
 * &quot;At 8:00am every Monday through Friday&quot; or &quot;At 1:30am every
 * last Friday of the month&quot;.
 * <P>
 * Cron expressions are comprised of 6 required fields and one optional field
 * separated by white space. The fields respectively are described as follows:
 *
 * <table cellspacing="8">
 * <tr>
 * <th align="left">Field Name</th>
 * <th align="left">&nbsp;</th>
 * <th align="left">Allowed Values</th>
 * <th align="left">&nbsp;</th>
 * <th align="left">Allowed Special Characters</th>
 * </tr>
 * <tr>
 * <td align="left"><code>Seconds (Optional)</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>empty, 0-59</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>, - * /</code></td>
 * </tr>
 * <tr>
 * <td align="left"><code>Minutes</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>0-59</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>, - * /</code></td>
 * </tr>
 * <tr>
 * <td align="left"><code>Hours</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>0-23</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>, - * /</code></td>
 * </tr>
 * <tr>
 * <td align="left"><code>Day-of-month</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>1-31</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>, - * ? / L W</code></td>
 * </tr>
 * <tr>
 * <td align="left"><code>Month</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>1-12 or JAN-DEC</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>, - * /</code></td>
 * </tr>
 * <tr>
 * <td align="left"><code>Day-of-Week</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>0-7 or SUN-SAT</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>, - * ? / L #</code></td>
 * </tr>
 * <tr>
 * <td align="left"><code>Year (Optional)</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>empty, 1970-2199</code></td>
 * <td align="left">&nbsp;</th>
 * <td align="left"><code>, - * /</code></td>
 * </tr>
 * </table>
 * <P>
 * The '*' character is used to specify all values. For example, &quot;*&quot;
 * in the minute field means &quot;every minute&quot;.
 * <P>
 * The '?' character is allowed for the day-of-month and day-of-week fields. It
 * is used to specify 'no specific value'. This is useful when you need to
 * specify something in one of the two fields, but not the other.
 * <P>
 * The '-' character is used to specify ranges For example &quot;10-12&quot; in
 * the hour field means &quot;the hours 10, 11 and 12&quot;.
 * <P>
 * The ',' character is used to specify additional values. For example
 * &quot;MON,WED,FRI&quot; in the day-of-week field means &quot;the days Monday,
 * Wednesday, and Friday&quot;.
 * <P>
 * The '/' character is used to specify increments. For example &quot;0/15&quot;
 * in the seconds field means &quot;the seconds 0, 15, 30, and 45&quot;. And
 * &quot;5/15&quot; in the seconds field means &quot;the seconds 5, 20, 35, and
 * 50&quot;. Specifying '*' before the '/' is equivalent to specifying 0 is the
 * value to start with. Essentially, for each field in the expression, there is
 * a set of numbers that can be turned on or off. For seconds and minutes, the
 * numbers range from 0 to 59. For hours 0 to 23, for days of the month 0 to 31,
 * and for months 1 to 12. The &quot;/&quot; character simply helps you turn on
 * every &quot;nth&quot; value in the given set. Thus &quot;7/6&quot; in the
 * month field only turns on month &quot;7&quot;, it does NOT mean every 6th
 * month, please note that subtlety.
 * <P>
 * The 'L' character is allowed for the day-of-month and day-of-week fields.
 * This character is short-hand for &quot;last&quot;, but it has different
 * meaning in each of the two fields. For example, the value &quot;L&quot; in
 * the day-of-month field means &quot;the last day of the month&quot; - day 31
 * for January, day 28 for February on non-leap years. If used in the
 * day-of-week field by itself, it simply means &quot;7&quot; or
 * &quot;SAT&quot;. But if used in the day-of-week field after another value, it
 * means &quot;the last xxx day of the month&quot; - for example &quot;6L&quot;
 * means &quot;the last friday of the month&quot;. You can also specify an
 * offset from the last day of the month, such as "L-3" which would mean the
 * third-to-last day of the calendar month. <i>When using the 'L' option, it is
 * important not to specify lists, or ranges of values, as you'll get
 * confusing/unexpected results.</i>
 * <P>
 * The 'W' character is allowed for the day-of-month field. This character is
 * used to specify the weekday (Monday-Friday) nearest the given day. As an
 * example, if you were to specify &quot;15W&quot; as the value for the
 * day-of-month field, the meaning is: &quot;the nearest weekday to the 15th of
 * the month&quot;. So if the 15th is a Saturday, the trigger will fire on
 * Friday the 14th. If the 15th is a Sunday, the trigger will fire on Monday the
 * 16th. If the 15th is a Tuesday, then it will fire on Tuesday the 15th.
 * However, if you specify &quot;1W&quot; as the value for day-of-month, and the
 * 1st is a Saturday, the trigger will fire on Monday the 3rd, as it will not
 * 'jump' over the boundary of a month's days. The 'W' character can only be
 * specified when the day-of-month is a single day, not a range or list of days.
 * <P>
 * The 'L' and 'W' characters can also be combined for the day-of-month
 * expression to yield 'LW', which translates to &quot;last weekday of the
 * month&quot;.
 * <P>
 * The '#' character is allowed for the day-of-week field. This character is
 * used to specify &quot;the nth&quot; XXX day of the month. For example, the
 * value of &quot;6#3&quot; in the day-of-week field means the third Friday of
 * the month (day 6 = Friday and &quot;#3&quot; = the 3rd one in the month).
 * Other examples: &quot;2#1&quot; = the first Monday of the month and
 * &quot;4#5&quot; = the fifth Wednesday of the month. Note that if you specify
 * &quot;#5&quot; and there is not 5 of the given day-of-week in the month, then
 * no firing will occur that month. If the '#' character is used, there can only
 * be one expression in the day-of-week field (&quot;3#1,6#3&quot; is not valid,
 * since there are two expressions).
 * <P>
 * <!--The 'C' character is allowed for the day-of-month and day-of-week fields.
 * This character is short-hand for "calendar". This means values are calculated
 * against the associated calendar, if any. If no calendar is associated, then
 * it is equivalent to having an all-inclusive calendar. A value of "5C" in the
 * day-of-month field means "the first day included by the calendar on or after
 * the 5th". A value of "1C" in the day-of-week field means
 * "the first day included by the calendar on or after Sunday".-->
 * <P>
 * The legal characters and the names of months and days of the week are not
 * case-sensitive.
 *
 * <p>
 * <b>NOTES:</b>
 * <ul>
 * <li>Support for specifying both a day-of-week and a day-of-month value is not
 * complete (you'll need to use the '?' character in one of these fields).</li>
 * <li>Overflowing ranges is supported - that is, having a larger number on the
 * left hand side than the right. You might do 22-2 to catch 10 o'clock at night
 * until 2 o'clock in the morning, or you might have NOV-FEB. It is very
 * important to note that overuse of overflowing ranges creates ranges that
 * don't make sense and no effort has been made to determine which
 * interpretation CronExpression chooses. An example would be
 * "0 0 14-6 ? * FRI-MON".</li>
 * </ul>
 * </p>
 *
 * @author cnotch
 */
public class CronExpression implements Serializable, Cloneable {

    private static final long serialVersionUID = 12423409423L;
    private static final int NOT_FOUNT_IDX = 64;

    private String expression;                     // raw expression string
    private final long[] years =
            new long[(YEAR.length + 63) >> 6];    // 0~230 bit
    private long seconds;               // 0~59 bit
    private long minutes;               // 0~59 bit
    private long hours;                 // 0~23 bit
    private long daysOfMonth;           // 1~31 bit
    private long months;                // 1~12 bit
    private long daysOfWeek;            // 1~35 bit(5 weeks)
    private long workdaysOfMonth;       // 1~31 bit
    private boolean lastDayOfMonth;     // L Flag
    private boolean lastWorkdayOfMonth; // LW Flag
    private long ithWeekdaysOfWeek;     // 1~35 bit(# sections)
    private long lastWeekdaysOfWeek;    // 1~35 bit(L sections)

    private CronExpression(String spec) {
        this.expression = spec;
    }

    public CronExpression(long seconds,
                          long minutes,
                          long hours,
                          long days,
                          long months,
                          long daysOfWeek) {
        this.seconds = seconds & SECOND.mask;
        this.minutes = minutes & MINUTE.mask;
        this.hours = hours & HOUR.mask;
        this.daysOfMonth = days & DAY_OF_MONTH.mask;
        this.months = months & MONTH.mask;
        this.daysOfWeek = daysOfWeek & DAY_OF_WEEK.mask;

        Arrays.fill(this.years, YEAR.mask);
        this.expression = "";
    }

    public String getExpression() {
        return this.expression;
    }

    /**
     * Returns the next date/time <I>after</I> the given date/time which
     * satisfies the cron expression.
     *
     * @param fromTime the date/time at which to begin the search for the next valid
     *                 date/time
     * @return the next valid date/time, returns null if there is no valid date/time
     */
    public Date next(Date fromTime) {
        return next(fromTime, null);
    }

    public Date next(Date fromTime, TimeZone timeZone) {
        DateTime dateTime = DateTime.of(fromTime, timeZone);
        dateTime = next(dateTime);

        return (dateTime == null) ? null : dateTime.toDate(timeZone);
    }

    /**
     * Returns the next date/time <I>after</I> the given date/time which
     * satisfies the cron expression.
     *
     * @param fromTime the date/time at which to begin the search for the next valid
     *                 date/time
     * @return the next valid date/time, returns null if there is no valid date/time
     */
    public LocalDateTime next(LocalDateTime fromTime) {
        DateTime dateTime = DateTime.of(fromTime);
        dateTime = next(dateTime);
        return (dateTime == null) ? null : dateTime.toLocalDateTime();
    }

    /**
     * Returns the next date/time <I>after</I> the given date/time which
     * satisfies the cron expression.
     *
     * @param fromTime the date/time at which to begin the search for the next valid
     *                 date/time
     * @return the next valid date/time, returns null if there is no valid date/time
     */
    public ZonedDateTime next(ZonedDateTime fromTime) {
        DateTime dateTime = DateTime.of(fromTime);
        dateTime = next(dateTime);
        return (dateTime == null) ? null : dateTime.toZonedDateTime(fromTime.getZone());
    }

    /**
     * Returns the next date/time <I>after</I> the given date/time which
     * satisfies the cron expression.
     *
     * @param fromTime the date/time at which to begin the search for the next valid
     *                 date/time
     * @return the next valid date/time, returns null if there is no valid date/time
     */
    public OffsetDateTime next(OffsetDateTime fromTime) {
        DateTime dateTime = DateTime.of(fromTime);
        dateTime = next(dateTime);
        return (dateTime == null) ? null : dateTime.toOffsetDateTime(fromTime.getOffset());
    }

    private DateTime next(DateTime fromTime) {
        // Since nextSecond()-nextMonth() expects that the
        // supplied time stamp is a perfect match to the underlying cron
        // expression, and since this function is an entry point where `fromTime`
        // does not necessarily match the underlying cron expression,
        // we first need to ensure supplied time stamp matches
        // the cron expression. If not, this means the supplied time
        // stamp falls in between matching time stamps, thus we move
        // to the closest future matching immediately upon encountering a mismatching
        // time stamp.

        // year
        int v = fromTime.getYear();
        int year = matchYear(v);
        if (year == 0) {
            return null;
        }

        if (v != year) {
            return nextYear(fromTime);
        }

        // month
        v = fromTime.getMonth();
        int i = matchField(months, MONTH.mask, v);
        if (i == NOT_FOUNT_IDX) {
            return nextYear(fromTime);
        }
        if (v != i) {
            return nextMonth(fromTime);
        }

        long actualDaysOfMonth = calculateActualDaysOfMonth(fromTime.getYear(), fromTime.getMonth());
        if (actualDaysOfMonth == 0) {
            return nextMonth(fromTime);
        }

        // day of month
        v = fromTime.getDayOfMonth();
        i = matchField(actualDaysOfMonth, DAY_OF_MONTH.mask, v);
        if (i == NOT_FOUNT_IDX) {
            return nextMonth(fromTime);
        }
        if (v != i) {
            return nextDayOfMonth(fromTime, actualDaysOfMonth);
        }

        // hour
        v = fromTime.getHour();
        i = matchField(hours, HOUR.mask, v);
        if (i == NOT_FOUNT_IDX) {
            return nextDayOfMonth(fromTime, actualDaysOfMonth);
        }
        if (v != i) {
            return nextHour(fromTime, actualDaysOfMonth);
        }

        // minute
        v = fromTime.getMinute();
        i = matchField(minutes, MINUTE.mask, v);
        if (i == NOT_FOUNT_IDX) {
            return nextHour(fromTime, actualDaysOfMonth);
        }
        if (v != i) {
            return nextMinute(fromTime, actualDaysOfMonth);
        }

        // second
        v = fromTime.getSecond();
        i = matchField(seconds, SECOND.mask, v);
        if (i == NOT_FOUNT_IDX) {
            return nextMinute(fromTime, actualDaysOfMonth);
        }

        // If we reach this point, there is nothing better to do
        // than to move to the next second
        return nextSecond(fromTime, actualDaysOfMonth);
    }

    private int matchYear(int year) {
        if (year > YEAR.max) {
            return 0;
        }
        if (year < YEAR.min) {
            year = YEAR.min;
        }

        int idx = year - YEAR.min;
        int bit = idx & 0x3f;
        for (int i = idx >> 6; i < this.years.length; i++) {
            int found = matchField(this.years[i], YEAR.mask, bit);
            if (found != NOT_FOUNT_IDX) {
                return (i << 6) + found + YEAR.min;
            }
            bit = 0;
        }
        return 0;
    }

    private int matchField(long v, long mask, int i) {
        return Long.numberOfLeadingZeros(v & ((mask << i) >>> i));
    }

    private int minValue(long v) {
        return Long.numberOfLeadingZeros(v);
    }

    private DateTime nextYear(DateTime t) {
        // Find index at which item in list is greater or equal to
        // candidate year
        int year = matchYear(t.getYear() + 1);
        if (year == 0) {
            return null;
        }

        // Year changed, need to recalculate actual days of month
        long actualDaysOfMonth = calculateActualDaysOfMonth(year, minValue(this.months));
        if (actualDaysOfMonth == 0) {
            return nextMonth(
                    t.set(year,
                            minValue(months),
                            1,
                            minValue(hours),
                            minValue(minutes),
                            minValue(seconds))
            );
        }
        return t.set(
                year,
                minValue(months),
                minValue(actualDaysOfMonth),
                minValue(hours),
                minValue(minutes),
                minValue(seconds));
    }

    private DateTime nextMonth(DateTime t) {
        // Find index at which item in list is greater or equal to
        // candidate month
        int i = matchField(months, MONTH.mask, t.getMonth() + 1);
        if (i == NOT_FOUNT_IDX) {
            return nextYear(t);
        }

        // Month changed, need to recalculate actual days of month
        long actualDaysOfMonth = calculateActualDaysOfMonth(t.getYear(), i);
        if (actualDaysOfMonth == 0) {
            return nextMonth(
                    t.set(t.getYear(),
                            i,
                            1,
                            minValue(hours),
                            minValue(minutes),
                            minValue(seconds)));
        }

        return t.setMonth(i).
                setDayOfMonth(minValue(actualDaysOfMonth)).
                setHour(minValue(hours)).
                setMinute(minValue(minutes)).
                setSecond(minValue(seconds));

    }

    private DateTime nextDayOfMonth(DateTime t, long actualDaysOfMonth) {
        // Find index at which item in list is greater or equal to
        // candidate day of month
        int i = matchField(actualDaysOfMonth, DAY_OF_MONTH.mask, t.getDayOfMonth() + 1);
        if (i == NOT_FOUNT_IDX) {
            return nextMonth(t);
        }

        return t.setDayOfMonth(i).
                setHour(minValue(hours)).
                setMinute(minValue(minutes)).
                setSecond(minValue(seconds));
    }

    private DateTime nextHour(DateTime t, long actualDaysOfMonth) {
        // Find index at which item in list is greater or equal to
        // candidate hour
        int i = matchField(hours, HOUR.mask, t.getHour() + 1);
        if (i == NOT_FOUNT_IDX) {
            return nextDayOfMonth(t, actualDaysOfMonth);
        }

        return t.setHour(i).setMinute(minValue(minutes)).setSecond(minValue(seconds));
    }

    private DateTime nextMinute(DateTime t, long actualDaysOfMonth) {
        // Find index at which item in list is greater or equal to
        // candidate minute
        int i = matchField(minutes, MINUTE.mask, t.getMinute() + 1);
        if (i == NOT_FOUNT_IDX) {
            return nextHour(t, actualDaysOfMonth);
        }

        return t.setMinute(i).setSecond(minValue(seconds));
    }

    private DateTime nextSecond(DateTime t, long actualDaysOfMonth) {
        // nextSecond() assumes all other fields are exactly matched
        // to the cron expression

        // Find index at which item in list is greater or equal to
        // candidate second
        int i = matchField(seconds, SECOND.mask, t.getSecond() + 1);
        if (i == NOT_FOUNT_IDX) {
            return nextMinute(t, actualDaysOfMonth);
        }

        return t.setSecond(i);
    }

    private long calculateActualDaysOfMonth(int year, int month) {
        int lastDay = DateTime.lengthOfMonth(year, month);
        // remove bits over lastDay
        long thisMonthsMask = (DAY_OF_MONTH.mask >>> (63 - lastDay)) << (63 - lastDay);

        // As per crontab man page (http://linux.die.net/man/5/crontab#):
        //  "The day of a command's execution can be specified by two
        //  "fields - day of month, and day of week. If both fields are
        //  "restricted (ie, aren't *), the command will be run when
        //  "either field matches the current time"

        // If both fields are not restricted, all days of the month are a hit
        if (daysOfMonth == DAY_OF_MONTH.mask && daysOfWeek == DAY_OF_WEEK.mask) {
            return thisMonthsMask;
        }

        int lastWeekday = DateTime.getDayOfWeek(year, month, lastDay);
        int firstWeekday = DateTime.getDayOfWeek(year, month, 1);
        long actualDaysOfMonth = 0L;
        // day-of-month != `*`
        if (daysOfMonth != DAY_OF_MONTH.mask) {
            // Days of month
            actualDaysOfMonth |= daysOfMonth;

            // Last day of month(L Flag)
            if (this.lastDayOfMonth) {
                actualDaysOfMonth |= START_BIT >>> lastDay;
            }
            // Last work day of month(LW Flag)
            if (this.lastWorkdayOfMonth) {
                int workday = lastWorkdayOfMonth(lastDay, lastWeekday);
                actualDaysOfMonth |= START_BIT >>> workday;
            }
            // Work days of month ({Number}W)
            // As per Wikipedia: month boundaries are not crossed.
            long workdaysOfMonth = this.workdaysOfMonth & thisMonthsMask;
            if (workdaysOfMonth > 0) {
                int start = Long.numberOfLeadingZeros(workdaysOfMonth);
                int end = 63 - Long.numberOfTrailingZeros(workdaysOfMonth);
                if (start == 1) {
                    int workday = firstWorkdayOfMonth(firstWeekday);
                    actualDaysOfMonth |= START_BIT >>> workday;
                    start++;
                }
                for (int v = start; v <= end && v < lastDay; v++) {
                    if ((workdaysOfMonth & (START_BIT >>> v)) != 0) {
                        int workday = midWorkdayOfMonth(v, (firstWeekday + v - 1) % 7);
                        actualDaysOfMonth |= START_BIT >>> workday;
                    }
                }
                if (end == lastDay) {
                    int workday = lastWorkdayOfMonth(lastDay, lastWeekday);
                    actualDaysOfMonth |= START_BIT >>> workday;
                }
            }
        }

        // day-of-week != `*`
        if (this.daysOfWeek != DAY_OF_WEEK.mask) {
            // days of week
            // daysOfWeek << to set bit 1 is the first day of month
            actualDaysOfMonth |= this.daysOfWeek << firstWeekday;

            // days of week of specific week in the month(4#2)
            // specificWeekDaysOfWeek << to set bit 1 is the first day of month
            actualDaysOfMonth |= this.ithWeekdaysOfWeek << firstWeekday;

            // Last days of week of the month({Weekday}L)
            // lastWeekDaysOfWeek << to set bit 1 is the first day of month
            long lastWeekdays = this.lastWeekdaysOfWeek << firstWeekday;
            // keep it for the last week
            lastWeekdays = (lastWeekdays << (lastDay - 7)) >>> (lastDay - 7);
            actualDaysOfMonth |= lastWeekdays;
        }

        // remove bits over lastDay
        return actualDaysOfMonth & thisMonthsMask;
    }

    private int lastWorkdayOfMonth(int lastDay, int lastWeekday) {
        switch (lastWeekday) {
            case 6: // Saturday
                return lastDay - 1;
            case 0: // Sunday
                return lastDay - 2;
            default:
                return lastDay;
        }
    }

    private int firstWorkdayOfMonth(int firstWeekday) {
        switch (firstWeekday) {
            case 6:// Saturday
                return 3;
            case 0: // Sunday
                return 2;
            default:
                return 1;
        }
    }

    private int midWorkdayOfMonth(int midDay, int weekday) {
        switch (weekday) {
            case 6: // Saturday
                return midDay - 1;
            case 0: // Sunday
                return midDay + 1;
            default:
                return midDay;
        }
    }
    @Override
    public CronExpression clone() {
        try {
            return (CronExpression) super.clone();
        } catch (final CloneNotSupportedException ex) {
            throw new InternalError();
        }
    }

    /**
     * Parse a new <CODE>CronExpression</CODE> based on the specified
     * parameter.
     *
     * @param spec String representation of the cron expression the new object
     *             should represent
     * @throws IllegalArgumentException if the string is null or empty
     * @throws ParseException           if the string expression cannot be parsed into a valid
     *                                            <CODE>CronExpression</CODE>
     */
    public static CronExpression parse(String spec) throws ParseException {
        if (spec == null) {
            throw new IllegalArgumentException("spec cannot be null");
        }

        String cron = spec.trim();
        if (cron.length() == 0) {
            throw new IllegalArgumentException("empty spec string");
        }

        // Handle named cron expression
        if (cron.startsWith("@")) {
            CronExpression expr = parseNamedExpression(cron);
            expr.expression = spec;
            return expr;
        }

        CronExpression expr = new CronExpression(spec);

        // remove empty field
        String[] validFields = Arrays.stream(cron.split(" "))
                .filter(s -> s.length() > 0).toArray(String[]::new);

        if (validFields.length < 5) {
            throw new ParseException(String.format("missing field(s): %s", spec), -1);
        }

        int fieldIndex = 0;
        int parserIndex = 0;
        Field[] allFields = Field.values();

        // second field (optional)
        if (validFields.length == 5) {
            expr.seconds = SECOND.toLongArray(0)[0]; // 0 second
            parserIndex++;                // set minute parser to the first
        }

        while (fieldIndex < validFields.length && parserIndex < allFields.length) {
            Field field = allFields[parserIndex];
            long[] bitSet = allFields[parserIndex].parse(validFields[fieldIndex]);
            fieldIndex++;
            parserIndex++;

            expr.setFieldValue(field, bitSet);
        }

        // padding years to all values
        if (validFields.length < 7) {
            Arrays.fill(expr.years, YEAR.mask);
        }

        // special handling for day of week
        expr.daysOfWeek = adjustWeekBits(expr.daysOfWeek);
        expr.lastWeekdaysOfWeek = adjustWeekBits(expr.lastWeekdaysOfWeek);
        return expr;
    }

    private static CronExpression parseNamedExpression(String spec) throws ParseException {
        switch (spec) {
            case "@yearly":
            case "@annually": //0 0 0 1 1 * *
                return new CronExpression(START_BIT, START_BIT, START_BIT,
                        START_BIT >>> DAY_OF_MONTH.min, START_BIT >>> MONTH.min, DAY_OF_WEEK.mask);
            case "@monthly": // 0 0 0 1 * * *
                return new CronExpression(START_BIT, START_BIT, START_BIT,
                        START_BIT >>> DAY_OF_MONTH.min, MONTH.mask, DAY_OF_WEEK.mask);
            case "@weekly": // 0 0 0 * * 0 *
                return new CronExpression(START_BIT, START_BIT, START_BIT,
                        DAY_OF_MONTH.mask, MONTH.mask, adjustWeekBits(DAY_OF_WEEK.toLongArray(0)[0]));
            case "@daily":
            case "@midnight": // 0 0 0 * * * *
                return new CronExpression(START_BIT, START_BIT, START_BIT,
                        DAY_OF_MONTH.mask, MONTH.mask, DAY_OF_WEEK.mask);
            case "@hourly": // 0 0 * * * * *
                return new CronExpression(START_BIT, START_BIT, HOUR.mask,
                        DAY_OF_MONTH.mask, MONTH.mask, DAY_OF_WEEK.mask);
        }

        throw new ParseException(String.format("unrecognized name of cron expression: %s", spec), -1);
    }

    private void setFieldValue(Field field, long[] values) {
        switch (field) {
            case SECOND:
                seconds = values[0];
                break;
            case MINUTE:
                minutes = values[0];
                break;
            case HOUR:
                hours = values[0];
                break;
            case DAY_OF_MONTH: {
                long v = values[0];
                lastWorkdayOfMonth = (v & (START_BIT >>> Field.DAY_OF_MONTH.length)) != 0L;
                lastDayOfMonth = (v & START_BIT) != 0L;
                daysOfMonth = v & DAY_OF_MONTH.mask;
                workdaysOfMonth = (v << Field.DAY_OF_MONTH.length) & DAY_OF_MONTH.mask;
                break;
            }
            case MONTH:
                months = values[0];
                break;
            case DAY_OF_WEEK: {
                long v = values[0];
                daysOfWeek = v & 0xff00000000000000L;
                lastWeekdaysOfWeek = (v << Field.DAY_OF_WEEK.length) & 0xff00000000000000L;
                ithWeekdaysOfWeek = (v << (DAY_OF_WEEK.length * 2)) & DAY_OF_WEEK.mask;
                break;
            }
            case YEAR:
                System.arraycopy(values, 0, years, 0, values.length);
                break;
        }
    }

    /**
     * CronExpression Date and time class used internally
     */
    private static class DateTime {
        private int year;
        private byte month;
        private byte day;
        private byte hour;
        private byte minute;
        private byte second;

        public DateTime(int year, int month, int day, int hour, int minute, int second) {
            set(year, month, day, hour, minute, second);
        }

        public LocalDateTime toLocalDateTime() {
            return LocalDateTime.of(year, month, day, hour, minute, second);
        }

        public ZonedDateTime toZonedDateTime(ZoneId zoneId) {
            return ZonedDateTime.of(year, month, day, hour, minute, second, 0, zoneId);
        }

        public OffsetDateTime toOffsetDateTime(ZoneOffset zoneOffset) {
            return OffsetDateTime.of(year, month, day, hour, minute, second, 0, zoneOffset);
        }

        public Date toDate(TimeZone timeZone) {
            if (timeZone == null) timeZone = TimeZone.getDefault();

            Calendar cl = new java.util.GregorianCalendar(timeZone);
            cl.clear();
            cl.set(Calendar.YEAR, year);
            cl.set(Calendar.MONTH, month - 1);
            cl.set(Calendar.DAY_OF_MONTH, day);
            cl.set(Calendar.HOUR_OF_DAY, hour);
            cl.set(Calendar.MINUTE, minute);
            cl.set(Calendar.SECOND, second);
            return cl.getTime();
        }

        public static DateTime of(LocalDateTime dateTime) {
            return new DateTime(
                    dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                    dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond()
            );
        }

        public static DateTime of(ZonedDateTime dateTime) {
            return new DateTime(
                    dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                    dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond()
            );
        }

        public static DateTime of(OffsetDateTime dateTime) {
            return new DateTime(
                    dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                    dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond()
            );
        }

        public static DateTime of(Date dateTime, TimeZone timeZone) {
            if (timeZone == null) timeZone = TimeZone.getDefault();

            Calendar cl = new java.util.GregorianCalendar(timeZone);
            cl.setTime(dateTime);

            return new DateTime(
                    cl.get(Calendar.YEAR),
                    cl.get(Calendar.MONTH) + 1,
                    cl.get(Calendar.DAY_OF_MONTH),
                    cl.get(Calendar.HOUR_OF_DAY),
                    cl.get(Calendar.MINUTE),
                    cl.get(Calendar.SECOND)
            );
        }

        public int getYear() {
            return year;
        }

        public int getMonth() {
            return month;
        }

        public int getDayOfMonth() {
            return day;
        }

        public int getDayOfWeek() {
            return getDayOfWeek(year, month, day);
        }

        public int getHour() {
            return hour;
        }

        public int getMinute() {
            return minute;
        }

        public int getSecond() {
            return second;
        }

        public DateTime set(int year, int month, int day, int hour, int minute, int second) {
            this.setYear(year).
                    setMonth(month).
                    setDayOfMonth(day).
                    setHour(hour).
                    setMinute(minute).
                    setSecond(second);
            return this;
        }

        public DateTime setYear(int year) {
            this.year = checkValidValue(year, Field.YEAR);
            return this;
        }

        public DateTime setMonth(int month) {
            this.month = (byte) checkValidValue(month, Field.MONTH);
            return this;
        }

        public DateTime setDayOfMonth(int day) {
            this.day = (byte) checkValidValue(day, Field.DAY_OF_MONTH);
            return this;
        }

        public DateTime setHour(int hour) {
            this.hour = (byte) checkValidValue(hour, Field.HOUR);
            return this;
        }

        public DateTime setMinute(int minute) {
            this.minute = (byte) checkValidValue(minute, Field.MINUTE);
            return this;
        }

        public DateTime setSecond(int second) {
            this.second = (byte) checkValidValue(second, Field.SECOND);
            return this;
        }

        private int checkValidValue(int value, Field field) {
            if (!field.isValidValue(value)) {
                throw new DateTimeException("Invalid value for " + field
                        + " (valid values " + field.min + "-" + field.max + "): "
                        + value);
            }
            return value;
        }

        /**
         * The number of days from year zero to year 1970.
         * There are five 400 year cycles from year zero to 2000.
         * There are 7 leap years from 1970 to 2000.
         */
        private static final long DAYS_0000_TO_1970 = (146097 * 5L) - (30L * 365L + 7L);

        public static long toEpochDay(int year, int month, int day) {
            long y = year;
            long m = month;
            long total = 0;
            total += 365 * y;
            if (y >= 0) {
                total += (y + 3) / 4 - (y + 99) / 100 + (y + 399) / 400;
            } else {
                total -= y / -4 - y / -100 + y / -400;
            }
            total += ((367 * m - 362) / 12);
            total += day - 1;
            if (m > 2) {
                total--;
                if (!isLeapYear(year)) {
                    total--;
                }
            }
            return total - DAYS_0000_TO_1970;
        }

        public static int getDayOfWeek(int year, int month, int day) {
            int dow0 = (int) Math.floorMod(toEpochDay(year, month, day) + 3, 7) + 1;
            // Sunday -> 0
            return dow0 == 7 ? 0 : dow0;
        }

        public static boolean isLeapYear(int year) {
            return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
        }

        public static int lengthOfMonth(int year, int month) {
            switch (month) {
                case 2:
                    return (isLeapYear(year) ? 29 : 28);
                case 4:
                case 6:
                case 9:
                case 11:
                    return 30;
                default:
                    return 31;
            }
        }
    }

    /**
     * Cron expression fields.
     */
    public enum Field {
        /**
         * 秒，0~59 bit
         */
        SECOND(0, 59),
        /**
         * 分钟，0~59 bit
         */
        MINUTE(0, 59),
        /**
         * 小时，0~23 bit
         */
        HOUR(0, 23),
        /**
         * 日，1~31 bit
         */
        DAY_OF_MONTH(1, 31),
        /**
         * 月，1~12 bit
         */
        MONTH(1, 12),
        /**
         * 周，覆盖一个月；1~35 bit(5 weeks)
         */
        DAY_OF_WEEK(0, 7),
        /**
         * 年，1970~2199.
         */
        YEAR(1970, 2199, false);

        public static final long START_BIT = 1L << 63;

        public final int min;
        public final int max;
        public final long mask;
        public final int length;

        Field(int min, int max) {
            this(min, max, true);
        }

        Field(int min, int max, boolean isAbsoluteIndexOfBits) {
            this.min = min;
            this.max = max;
            this.length = max - (isAbsoluteIndexOfBits ? 0 : min) + 1;

            if (min == 0 && max == 7) {
                // weekday 1-35 special handling
                this.mask = 0xffffffffffffffffL << (64 - (35 - 1 + 1)) >>> 1;
            } else if (max < 63) {
                this.mask = 0xffffffffffffffffL << (64 - (max - min + 1)) >>> min;
            } else {
                // years special handling
                this.mask = 0xffffffffffffffffL;
            }
        }

        /**
         * 转换字串到字段有效值，如果字串无法转换返回 -1。
         */
        public int intValue(String s) {
            switch (this) {
                case MONTH:
                    return atomi(s);
                case DAY_OF_WEEK:
                    return atowi(s);
                default:
                    return atoi(s);
            }
        }

        /**
         * 判断指定数值是否是有效的字段值。
         */
        public boolean isValidValue(int n) {
            return n >= min && n <= max;
        }

        /**
         * 获取字段指定值数组的位模式的值集合。
         * 无效值自动排除，不参与计算。
         */
        public long[] toLongArray(int... ints) {
            if (this == YEAR) {
                long[] arr = new long[(YEAR.length + 63) >> 6];
                for (int n : ints) {
                    if (isValidValue(n)) {
                        int i = n - YEAR.min;
                        arr[i >> 6] |= START_BIT >>> (i & 0x3f);
                    }
                }
                return arr;
            } else {
                long v = 0L;
                for (int n : ints) {
                    if (isValidValue(n)) {
                        v |= START_BIT >>> n;
                    }
                }
                return new long[]{v};
            }
        }

        public static long adjustWeekBits(long v) {
            // special handling for day of week
            // 7->0
            if ((v & (START_BIT >>> 7)) != 0) {
                v |= START_BIT;
            }

            // expand to 5 week
            long mask = 0xfe00000000000000L;
            long daysOfWeek = v & mask;
            for (int i = 0; i < 35; i += 7) {
                v |= daysOfWeek >>> i;
            }

            // sun to bit 1
            v >>>= 1;
            return v;
        }

        /**
         * 对于 DaysOfMonth；0：表示是否是LastDay；1-31： 表示月中的日期；32：表示是否是LastWorkday；33-63：表示月中的工作日。
         * <p></p>
         * 对于 DaysOfWeek; 0-7: 表示星期中的日期；8-15：表示月中的LastWeekday；16-51：表示第几周的星期几。
         */
        public long[] parse(String field) throws ParseException {
            long[] bits = new long[this == YEAR ? (YEAR.length + 63) >> 6 : 1];

            int idx = field.indexOf(',');
            if (idx == -1) {
                parseEntry(bits, field);
                return bits;
            }

            String[] entries = field.split(",");
            for (String entry : entries) {
                parseEntry(bits, entry);
            }
            return bits;
        }

        private void parseEntry(long[] bits, String entry) throws ParseException {
            if (entry.equals("*")) { // min-max
                populateTo(bits, min, max, 1);
                return;
            }

            String errPattern = "syntax error in %s[" + this.min + "," + this.max + "] field, contains an invalid entry: '%s'";

            int n = this.intValue(entry);
            if (n != -1) { // one value
                populateTo(bits, n, n, 1);
                return;
            }

            // step  /
            int idx = entry.indexOf('/');
            if (idx != -1) {
                int step = this.intValue(entry.substring(idx + 1));
                if (step < 1 || step > (this.max - this.min)) {
                    throw new ParseException(String.format(errPattern, this.name(), entry), -1);
                }
                if (!parseStep(bits, entry.substring(0, idx), step)) {
                    throw new ParseException(String.format(errPattern, this.name(), entry), -1);
                }
                return;
            }

            // span
            idx = entry.indexOf('-');
            if (idx != -1) {
                if (!parseStep(bits, entry, 1)) {
                    throw new ParseException(String.format(errPattern, this.name(), entry), -1);
                }
                return;
            }

            boolean ok = false;
            switch (this) {
                case DAY_OF_MONTH:
                    ok = parseSpecDomEntry(bits, entry);
                    break;
                case DAY_OF_WEEK:
                    ok = parseSpecDowEntry(bits, entry);
                    break;
            }
            if (!ok) {
                throw new ParseException(String.format(errPattern, this.name(), entry), -1);
            }
        }

        private boolean parseStep(long[] bits, String entry, int step) {
            if (entry.equals("*")) { // min-max
                populateTo(bits, this.min, this.max, step);
                return true;
            }

            int n = this.intValue(entry);
            if (n != -1) {
                populateTo(bits, n, this.max, step);
                return true;
            }

            // standard begin-end
            int idx = entry.indexOf('-');
            int begin = this.intValue(entry.substring(0, idx));
            if (begin == -1) {
                return false;
            }
            int end = this.intValue(entry.substring(idx + 1));
            if (end == -1 || (begin > end && this == YEAR)) {
                return false;
            }
            populateTo(bits, begin, end, step);
            return true;
        }

        private void populateTo(long[] bits, int begin, int end, int step) {
            if (this == YEAR) {
                for (int i = begin - YEAR.min; i <= end - YEAR.min; i += step) {
                    bits[i >> 6] |= START_BIT >>> (i & 0x3f);
                }
                return;
            }

            long v = 0;
            if (begin <= end) {
                for (int i = begin; i <= end; i += step) {
                    v |= START_BIT >>> i;
                }
                bits[0] |= v;
                return;
            }

            // handle overflowing ranges
            int i = begin;
            for (; i <= this.max; i += step) {
                v |= START_BIT >>> i;
            }

            i = i - (this.max + 1) + (this == DAY_OF_WEEK ? 1 : this.min);
            for (; i <= end; i += step) {
                v |= START_BIT >>> i;
            }

            bits[0] |= v;
        }

        private boolean parseSpecDomEntry(long[] bits, String entry) {
            if (entry.equals("?")) {
                bits[0] |= DAY_OF_MONTH.mask;
                return true;
            }

            if (entry.equals("LW")) {
                bits[0] |= START_BIT >>> DAY_OF_MONTH.length;
                return true;
            }

            if (entry.equals("L")) {
                bits[0] |= START_BIT;
                return true;
            }

            if (entry.endsWith("W")) {
                int n = DAY_OF_MONTH.intValue(entry.substring(0, entry.length() - 1));
                if (n == -1) {
                    return false;
                }
                bits[0] |= START_BIT >>> (DAY_OF_MONTH.length + n);
                return true;
            }
            return false;
        }

        private boolean parseSpecDowEntry(long[] bits, String entry) {
            if (entry.equals("?")) {
                bits[0] |= DAY_OF_WEEK.mask << 1;
                return true;
            }

            if (entry.endsWith("L")) {
                int n = DAY_OF_WEEK.intValue(entry.substring(0, entry.length() - 1));
                if (n == -1) {
                    return false;
                }
                bits[0] |= START_BIT >>> (DAY_OF_WEEK.length + n);
                return true;
            }

            int idx = entry.indexOf('#');
            if (idx != -1) {
                int weekday = DAY_OF_WEEK.intValue(entry.substring(0, idx));
                if (weekday == -1) {
                    return false;
                }
                int ith = DAY_OF_WEEK.intValue(entry.substring(idx + 1));
                if (ith < 1 || ith > 5) {
                    return false;
                }
                if (weekday == 7) {
                    weekday = 0;
                }
                int n = (ith - 1) * 7 + weekday;
                bits[0] |= START_BIT >>> (DAY_OF_WEEK.length * 2 + n + 1);// sun is bit 1
                return true;
            }

            return false;
        }

        private int atoi(String s) {
            try {
                int v = Integer.parseInt(s);
                return isValidValue(v) ? v : -1;
            } catch (NumberFormatException e) {
                return -1;
            }
        }

        private int atomi(String s) {
            switch (s.toLowerCase()) {
                case "january":
                case "jan":
                case "1":
                    return 1;
                case "february":
                case "feb":
                case "2":
                    return 2;
                case "march":
                case "mar":
                case "3":
                    return 3;
                case "april":
                case "apr":
                case "4":
                    return 4;
                case "may":
                case "5":
                    return 5;
                case "june":
                case "jun":
                case "6":
                    return 6;
                case "july":
                case "jul":
                case "7":
                    return 7;
                case "august":
                case "aug":
                case "8":
                    return 8;
                case "september":
                case "sep":
                case "9":
                    return 9;
                case "october":
                case "oct":
                case "10":
                    return 10;
                case "november":
                case "nov":
                case "11":
                    return 11;
                case "december":
                case "dec":
                case "12":
                    return 12;
                default:
                    return -1;
            }
        }

        private int atowi(String s) {
            switch (s.toLowerCase()) {
                case "monday":
                case "mon":
                case "1":
                    return 1;
                case "tuesday":
                case "tue":
                case "2":
                    return 2;
                case "wednesday":
                case "wed":
                case "3":
                    return 3;
                case "thursday":
                case "thu":
                case "4":
                    return 4;
                case "friday":
                case "fri":
                case "5":
                    return 5;
                case "saturday":
                case "sat":
                case "6":
                    return 6;
                case "7":
                    return 7;
                case "sunday":
                case "sun":
                case "0":
                    return 0;
                default:
                    return -1;
            }
        }
    }

    public <V> ScheduledFuture<V> schedule(ScheduledExecutorService pool,Callable<V> callable){
        if (pool == null) {
            throw new NullPointerException("pool");
        }
        if (callable == null) {
            throw new NullPointerException("callable");
        }

        TaskFuture<V> future = new TaskFuture<>(pool, callable);
        future.schedule();
        return future;
    }

    public ScheduledFuture<?> schedule(ScheduledExecutorService pool,Runnable command){
        if (pool == null) {
            throw new NullPointerException("pool");
        }
        if (command == null) {
            throw new NullPointerException("command");
        }

        TaskFuture<Void> future = new TaskFuture<>(pool,command);
        future.schedule();
        return future;
    }

    private class TaskFuture<V> implements ScheduledFuture<V>, Callable<V> {

        private final AtomicReference<ScheduledFuture<V>> future;
        private final ScheduledExecutorService pool;
        private final Callable<V> callable;

        private LocalDateTime triggerTime;
        private boolean canceled;

        public TaskFuture(ScheduledExecutorService pool,Runnable command) {
            this.future = new AtomicReference<>();
            this.pool = pool;
            this.callable = Executors.callable(command, null);
            this.triggerTime = LocalDateTime.now();
            this.canceled = false;
        }

        public TaskFuture(ScheduledExecutorService pool,Callable<V> callable) {
            this.future = new AtomicReference<>();
            this.pool = pool;
            this.callable = callable;
            this.triggerTime = LocalDateTime.now();
            this.canceled = false;
        }

        @Override
        public V call() throws Exception {
            try {
                return callable.call();
            } finally {
                schedule();
            }
        }

        @Override
        public int compareTo(Delayed o) {
            return this.future.get().compareTo(o);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return this.future.get().getDelay(unit);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            this.canceled = true;
            return this.future.get().cancel(mayInterruptIfRunning);
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return this.future.get().get();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException,
                ExecutionException, TimeoutException {
            return this.future.get().get(timeout, unit);
        }

        @Override
        public boolean isCancelled() {
            return this.canceled || this.future.get().isCancelled();
        }

        @Override
        public boolean isDone() {
            return this.future.get().isDone();
        }

        private void schedule() {
            long delay = nextDelay();

            if ((delay >= 0) && !canceled) {
                this.future.set(pool.schedule(this, delay,
                        TimeUnit.MILLISECONDS));
            }
        }

        private long nextDelay() {
            LocalDateTime nextDate = next(this.triggerTime);
            if (nextDate == null) {
                return -1;
            }

            this.triggerTime = nextDate;
            Duration duration = Duration.between(LocalDateTime.now(),nextDate);

            // 执行时间过长的处理，立即执行
            if (duration.isNegative()) {
                return 0;
            }
            return duration.toMillis();
        }
    }

}