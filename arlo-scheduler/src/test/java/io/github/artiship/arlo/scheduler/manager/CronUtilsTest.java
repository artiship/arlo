package io.github.artiship.arlo.scheduler.manager;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;

import static io.github.artiship.arlo.utils.CronUtils.calTimeRangeNew;
import static io.github.artiship.arlo.utils.CronUtils.calTimeRangeStr;
import static io.github.artiship.arlo.utils.Dates.localDateTime;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class CronUtilsTest {

    private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Test
    public void when_cron_is_week() throws ParseException {
        String cron = "00 18 00 ? * 2";
        LocalDateTime scheduleTime = parse("2020-08-24 00:18:00");

        assertThat(calTimeRangeNew(scheduleTime, cron)).isEqualTo(asList(
                parse("2020-08-17 00:00:00"),
                parse("2020-08-24 00:00:00"))
        );
    }

    @Test
    public void when_cron_is_odd() throws ParseException {
        String p = "00 00 18,8 * * ?";

        String s = calTimeRangeStr(parse("2020-02-02 18:00:00"), p);

        assertThat(s).isEqualTo("(2020-02-02 08:00:00~2020-02-02 18:00:00]");

        String s2 = calTimeRangeStr(parse("2020-02-02 08:00:00"), p);

        assertThat(s2).isEqualTo("(2020-02-01 18:00:00~2020-02-02 08:00:00]");

        log.info("{}", s2);
    }

    @Test
    public void when_cron_is_odd_2() throws ParseException {
        String parentCron = "00 00 01-02/1 * * ?";

        String s = calTimeRangeStr(parse("2019-12-10 01:00:00"), parentCron);

        log.info("{}", s);
    }

    private LocalDateTime parse(String dateStr) throws ParseException {
        return localDateTime(formatter.parse(dateStr));
    }
}
