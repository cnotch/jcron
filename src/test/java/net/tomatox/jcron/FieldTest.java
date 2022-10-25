package net.tomatox.jcron;

import org.junit.Test;

import java.text.ParseException;
import java.time.DayOfWeek;

import static net.tomatox.jcron.CronExpression.Field.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class FieldTest {
    @Test
    public void testMask() {
        assertEquals(0xfffffffffffffff0L, SECOND.mask);
        assertEquals(0xfffffffffffffff0L, MINUTE.mask);
        assertEquals(0xffffff0000000000L, HOUR.mask);
        assertEquals(0x7fffffff00000000L, DAY_OF_MONTH.mask);
        assertEquals(0x7ff8000000000000L, MONTH.mask);
        assertEquals(0x7ffffffff0000000L, DAY_OF_WEEK.mask);
    }

    @Test
    public void testIntValue() {
        assertEquals(2, MONTH.intValue("february"));
        assertEquals(2, MONTH.intValue("February"));
        assertEquals(2, MONTH.intValue("Feb"));
        assertEquals(2, MONTH.intValue("2"));

        assertEquals(2, DAY_OF_WEEK.intValue("tuesday"));
        assertEquals(2, DAY_OF_WEEK.intValue("tue"));
    }

    @Test
    public void testParseSection() throws ParseException {
        assertArrayEquals(new long[]{SECOND.mask}, SECOND.parse("*"));
        assertArrayEquals(SECOND.toLongArray(25), SECOND.parse("25"));
        assertArrayEquals(SECOND.toLongArray(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
                SECOND.parse("10-20"));
        assertArrayEquals(SECOND.toLongArray(10, 12, 14, 16, 18, 20), SECOND.parse("10-20/2"));
        assertArrayEquals(SECOND.toLongArray(50, 52, 54, 56, 58), SECOND.parse("50/2"));
        assertArrayEquals(SECOND.toLongArray(50, 52, 54, 56, 58, 0, 2), SECOND.parse("50-2/2"));

        assertArrayEquals(DAY_OF_MONTH.toLongArray(28, 29, 30, 31, 1, 2), DAY_OF_MONTH.parse("28-2"));
        assertArrayEquals(new long[]{START_BIT >>> DAY_OF_MONTH.length}, DAY_OF_MONTH.parse("LW"));
        assertArrayEquals(new long[]{START_BIT}, DAY_OF_MONTH.parse("L"));
        assertArrayEquals(new long[]{START_BIT >>> (DAY_OF_MONTH.length + 5)}, DAY_OF_MONTH.parse("5W"));

        assertArrayEquals(MONTH.toLongArray(11, 12, 1, 2), MONTH.parse("11-2"));

        assertArrayEquals(DAY_OF_WEEK.toLongArray(5, 6, 7, 1, 2), DAY_OF_WEEK.parse("fri-tue"));
        assertArrayEquals(new long[]{START_BIT >>> DayOfWeek.MONDAY.getValue()}, DAY_OF_WEEK.parse("MON"));
        assertArrayEquals(new long[]{START_BIT >>> (DAY_OF_WEEK.length + 5)}, DAY_OF_WEEK.parse("5L"));
        assertArrayEquals(new long[]{START_BIT >>> (DAY_OF_WEEK.length * 2 + 14 + 5 + 1)}, DAY_OF_WEEK.parse("5#3"));

        assertArrayEquals(new long[]{YEAR.mask, YEAR.mask, YEAR.mask, 0xfffffffffc000000L},
                YEAR.parse("*"));
        assertArrayEquals(YEAR.toLongArray(2020), YEAR.parse("2020"));
    }
}
