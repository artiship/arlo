package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.scheduler.manager.dependency.DefaultDependencyResolver;
import io.github.artiship.arlo.utils.CronUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.List;

import static com.google.common.base.Joiner.on;
import static io.github.artiship.arlo.scheduler.manager.dependency.DefaultDependencyResolver.builder;
import static io.github.artiship.arlo.utils.Dates.localDateTime;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DefaultDependencyResolverTest {

    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Test
    public void child_hour_parent_hour() throws ParseException {
        String childCron = "0 0 2,3,4,5,6,7 * * ?";
        String parentCron = "0 0 3,4,5,6,7 12,13 * ?";


        DefaultDependencyResolver builder = DefaultDependencyResolver.builder()
                                                     .childCronExpression(childCron)
                                                     .parentCronExpression(parentCron)
                                                     .childScheduleTime(parse("2020-09-08 04:00:00"))
                                                     .build();
        List<LocalDateTime> localDateTimes = builder.parentScheduleTimes();
        localDateTimes.stream()
                      .forEach(System.out::println);
    }

    @Test
    public void when_week_depends_week() throws ParseException {
        String parentCron = "00 0 6 ? * 2";
        String childCron = "00 0 6 ? * 2";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2020-09-07 06:00:00"))
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(
                parse("2020-09-07 06:00:00")
        ));
    }

    @Test
    public void when_day_depends_week() throws ParseException {
        String parentCron = "00 18 00 ? * 2";
        String childCron = "00 12 08 * * ?";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2020-08-25 08:12:00"))
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList((
                parse("2020-08-24 00:18:00")
        )));
    }

    @Test
    public void when_hour_depends_10_min() throws ParseException {
        String parentCron = "00 */10 00-23 * * ?";

        String childCron = "00 16 00-23/1 * * ?";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2020-03-26 09:16:00"))
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(
                parse("2020-03-26 08:10:00"),
                parse("2020-03-26 08:20:00"),
                parse("2020-03-26 08:30:00"),
                parse("2020-03-26 08:40:00"),
                parse("2020-03-26 08:50:00"),
                parse("2020-03-26 09:00:00")));

    }

    @Test
    public void when_cron_is_discrete() throws ParseException {
        String parentCron = "00 0 4,9,14 * * ?";
        String childCron = "00 0 4,9,14 * * ?";

        DefaultDependencyResolver checker1 = builder().parentCronExpression(parentCron)
                                              .childCronExpression(childCron)
                                              .childScheduleTime(parse("2020-03-26 09:00:00"))
                                              .build();

        assertThat(checker1.parentScheduleTimes()).isEqualTo(asList(parse("2020-03-26 09:00:00")));

        DefaultDependencyResolver checker2 = builder().parentCronExpression(parentCron)
                                              .childCronExpression(childCron)
                                              .childScheduleTime(parse("2020-03-26 14:00:00"))
                                              .build();

        assertThat(checker2.parentScheduleTimes()).isEqualTo(asList(parse("2020-03-26 14:00:00")));
    }

    @Test
    public void when_same_cycle_but_different_start_time_1() throws ParseException {
        String parentCron = "00 */10 * * * ?";
        String childCron = "00 1/10 * * * ?";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2020-03-23 15:01:00"))
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(parse("2020-03-23 15:00:00")));
    }

    @Test
    public void when_same_cycle_but_different_start_time_2() throws ParseException {
        String parentCron = "00 00 0/2 * * ?";
        String childCron = "00 00 1/2 * * ?";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2020-03-23 03:00:00"))
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(parse("2020-03-23 02:00:00")));

    }

    @Test
    public void when_recursive_is_deep() throws Exception {
        String parentCron = "01 */5 13-23 * * ?";
        String childCron = "00 */5 13-23 * * ?";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2019-12-11 13:00:00"))
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(parse("2019-12-11 13:00:01")));
    }

    @Test
    public void when_recursive_is_deep_2() throws Exception {
        String parentCron = "00 */5 13-23 * * ?";
        String childCron = "02 */5 13-23 * * ?";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2019-12-11 13:00:02"))
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(parse("2019-12-11 13:00:00")));
    }

    @Test
    public void when_recursive_is_deep_3() throws Exception {
        String parentCron = "00 */5 13-23 * * ?";
        String childCron = "00 */5 13-23 * * ?";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2019-12-11 13:00:00"))
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(parse("2019-12-11 13:00:00")));
    }

    @Test
    public void child_day_before_parent_day() throws Exception {
        String parentCron = "0 2 3 * * ?";
        String childCron = "0 0 0 * * ?";


        LocalDateTime parentScheduleTime = parse("2019-12-11 03:02:00");

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2019-12-11 00:00:00"))
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(parentScheduleTime));
    }

    @Test
    public void child_day_after_parent_day() throws Exception {
        String parentCron = "0 0 0 * * ?";
        String childCron = "0 2 3 * * ?";

        LocalDateTime childScheduleTime = parse("2019-12-11 03:02:00");
        LocalDateTime parentScheduleTime = parse("2019-12-11 00:00:00");

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(childScheduleTime)
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(parentScheduleTime));
    }

    @Test
    public void child_day_same_as_parent_day() throws Exception {
        String parentCron = "0 2 3 * * ?";
        String childCron = "0 2 3 * * ?";

        LocalDateTime childScheduleTime = parse("2019-12-11 03:02:00");
        LocalDateTime parentScheduleTime = parse("2019-12-11 03:02:00");

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(childScheduleTime)
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(parentScheduleTime));
    }

    @Test
    public void child_hour_before_parent_day() throws Exception {
        String parentCron = "0 2 3 * * ?";
        String childCron = "0 0 * * * ?";

        LocalDateTime childScheduleTime = parse("2019-12-11 00:00:00");
        LocalDateTime parentScheduleTime = parse("2019-12-11 03:02:00");

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(childScheduleTime)
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(parentScheduleTime));
    }

    @Test
    public void child_hour_after_parent_day() throws Exception {
        String parentCron = "0 0 0 * * ?";
        String childCron = "0 2 * * * ?";

        LocalDateTime childScheduleTime = parse("2019-12-11 00:02:00");
        LocalDateTime parentScheduleTime = parse("2019-12-11 00:00:00");

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(childScheduleTime)
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(parentScheduleTime));
    }

    @Test
    public void child_hour_same_parent_day() throws Exception {
        String parentCron = "0 2 0 * * ?";
        String childCron = "0 2 * * * ?";

        LocalDateTime childScheduleTime = parse("2019-12-11 00:02:00");
        LocalDateTime parentScheduleTime = parse("2019-12-11 00:02:00");

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(childScheduleTime)
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(parentScheduleTime));
    }

    @Test
    public void child_day_after_parent_hour_1_to_hour_2() throws Exception {
        String parentCron = "00 00 01-02/1 * * ?";
        String childCron = "1 0 3 * * ?";

        LocalDateTime childScheduleTime = parse("2019-12-11 03:00:01");

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(childScheduleTime)
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(
                parse("2019-12-10 01:00:00"),
                parse("2019-12-10 02:00:00")
        ));
    }

    @Test
    public void child_day_parent_hour_1_and_13() throws Exception {
        String parentCron = "4 1 1,13 * * ?";
        String childCron = "3 1 3 * * ?";

        LocalDateTime childScheduleTime = parse("2019-12-11 03:01:03");

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(childScheduleTime)
                                             .build();

        assertThat(checker.parentScheduleTimes()).isEqualTo(asList(
                parse("2019-12-10 13:01:04"),
                parse("2019-12-11 01:01:04")
        ));
    }

    @Test
    public void child_hour_parent_hour_1_and_13() throws Exception {
        String parentCron = "4 1 1,13 * * ?";
        String childCron = "3 1 */1 * * ?";

        DefaultDependencyResolver.Builder builder = builder().parentCronExpression(parentCron)
                                                     .childCronExpression(childCron);

        DefaultDependencyResolver checker_1 = builder.childScheduleTime(parse("2019-12-11 01:01:03"))
                                             .build();

        assertThat(checker_1.parentScheduleTimes()).isEqualTo(asList(parse("2019-12-11 01:01:04")));

        DefaultDependencyResolver checker_2 = builder.childScheduleTime(parse("2019-12-11 02:01:03"))
                                             .build();

        assertThat(checker_2.parentScheduleTimes()).isEqualTo(asList(parse("2019-12-11 01:01:04")));

        DefaultDependencyResolver checker_3 = builder.childScheduleTime(parse("2019-12-11 12:01:03"))
                                             .build();

        assertThat(checker_3.parentScheduleTimes()).isEqualTo(asList(parse("2019-12-11 01:01:04")));

        DefaultDependencyResolver checker_4 = builder.childScheduleTime(parse("2019-12-11 13:01:03"))
                                             .build();

        assertThat(checker_4.parentScheduleTimes()).isEqualTo(asList(parse("2019-12-11 13:01:04")));
    }

    @Test
    public void child_hour_3_to_4_parent_hour_1_to_2() throws Exception {
        String parentCron = "00 00 01-02/1 * * ?";
        String childCron = "00 00 03-04/1 * * ?";

        DefaultDependencyResolver.Builder builder = builder().parentCronExpression(parentCron)
                                                     .childCronExpression(childCron);

        DefaultDependencyResolver checker_1 = builder.childScheduleTime(parse("2019-12-11 03:00:00"))
                                             .build();

        List<LocalDateTime> expect = asList(parse("2019-12-11 02:00:00"));

        assertThat(checker_1.parentScheduleTimes()).isEqualTo(expect);

        DefaultDependencyResolver checker_2 = builder.childScheduleTime(parse("2019-12-11 04:00:00"))
                                             .build();

        assertThat(checker_2.parentScheduleTimes()).isEqualTo(expect);
    }

    @Test
    public void child_5_min_hour_1_to_2_parent_5_min_hour_20() throws Exception {
        String parentCron = "00 */5 20-20 * * ?";
        String childCron = "00 */5 01-02 * * ?";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2019-12-11 01:05:00"))
                                             .build();

        List<LocalDateTime> expect = asList(parse("2019-12-10 20:55:00"));

        assertThat(checker.parentScheduleTimes()).isEqualTo(expect);
    }

    @Test
    public void child_10_min_hour_1_to_2_parent_5_min_hour_20() throws Exception {
        String parentCron = "00 */5 20-20 * * ?";
        String childCron = "00 */10 01-02 * * ?";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2019-12-11 01:10:00"))
                                             .build();

        List<LocalDateTime> expect = asList(parse("2019-12-10 20:55:00"));

        assertThat(checker.parentScheduleTimes()).isEqualTo(expect);
    }

    @Test
    public void child_10_min_hour_20_parent_5_min_hour_1_to_2() throws Exception {
        String parentCron = "00 */5 01-02 * * ?";
        String childCron = "00 */10 20-20 * * ?";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2019-12-11 20:00:00"))
                                             .build();

        List<LocalDateTime> expect = asList(parse("2019-12-11 02:55:00"));

        assertThat(checker.parentScheduleTimes()).isEqualTo(expect);
    }

    @Test
    public void child_5_min_hour_1_to_2_parent_10_min_hour_20() throws Exception {
        String parentCron = "00 */10 20-20 * * ?";
        String childCron = "00 */5 01-02 * * ?";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2019-12-11 01:05:00"))
                                             .build();

        List<LocalDateTime> expect = asList(parse("2019-12-10 20:50:00"));

        assertThat(checker.parentScheduleTimes()).isEqualTo(expect);
    }

    @Test
    public void child_hour_4_to_5_parent_hour_1_to_2() throws Exception {
        String parentCron = "00 00 01-02/1 * * ?";
        String childCron = "00 00 04-05/1 * * ?";

        DefaultDependencyResolver checker_1 = builder().parentCronExpression(parentCron)
                                               .childCronExpression(childCron)
                                               .childScheduleTime(parse("2019-12-11 04:00:00"))
                                               .build();

        List<LocalDateTime> expect_1 = asList(
                parse("2019-12-11 02:00:00")
        );

        assertThat(checker_1.parentScheduleTimes()).isEqualTo(expect_1);

        DefaultDependencyResolver checker_2 = builder().parentCronExpression(parentCron)
                                               .childCronExpression(childCron)
                                               .childScheduleTime(parse("2019-12-11 05:00:00"))
                                               .build();

        List<LocalDateTime> expect_2 = asList(
                parse("2019-12-11 02:00:00")
        );

        assertThat(checker_2.parentScheduleTimes()).isEqualTo(expect_2);
    }

    @Test
    public void child_day_before_parent_hour() throws Exception {
        String parentCron = "0 3 * * * ?";
        String childCron = "0 2 0 * * ?";

        LocalDateTime childScheduleTime = parse("2019-12-11 00:02:00");

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(childScheduleTime)
                                             .build();

        List<LocalDateTime> expect = asList(
                parse("2019-12-10 01:03:00"),
                parse("2019-12-10 02:03:00"),
                parse("2019-12-10 03:03:00"),
                parse("2019-12-10 04:03:00"),
                parse("2019-12-10 05:03:00"),
                parse("2019-12-10 06:03:00"),
                parse("2019-12-10 07:03:00"),
                parse("2019-12-10 08:03:00"),
                parse("2019-12-10 09:03:00"),
                parse("2019-12-10 10:03:00"),
                parse("2019-12-10 11:03:00"),
                parse("2019-12-10 12:03:00"),
                parse("2019-12-10 13:03:00"),
                parse("2019-12-10 14:03:00"),
                parse("2019-12-10 15:03:00"),
                parse("2019-12-10 16:03:00"),
                parse("2019-12-10 17:03:00"),
                parse("2019-12-10 18:03:00"),
                parse("2019-12-10 19:03:00"),
                parse("2019-12-10 20:03:00"),
                parse("2019-12-10 21:03:00"),
                parse("2019-12-10 22:03:00"),
                parse("2019-12-10 23:03:00"),
                parse("2019-12-11 00:03:00"));

        assertThat(checker.parentScheduleTimes()).isEqualTo(expect);
    }

    @Test
    public void child_hour_before_parent_hour() throws Exception {
        String parentCron = "0 3 0 * * ?";
        String childCron = "0 2 0 * * ?";

        LocalDateTime childScheduleTime = parse("2019-12-11 00:02:00");

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(childScheduleTime)
                                             .build();


        List<LocalDateTime> expect = asList(parse("2019-12-11 00:03:00"));

        assertThat(checker.parentScheduleTimes()).isEqualTo(expect);
    }

    @Test
    public void child_hour_after_parent_hour() throws Exception {
        String parentCron = "0 2 0 * * ?";
        String childCron = "0 3 0 * * ?";

        LocalDateTime childScheduleTime = parse("2019-12-11 00:03:00");

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(childScheduleTime)
                                             .build();

        List<LocalDateTime> expect = asList(parse("2019-12-11 00:02:00"));

        assertThat(checker.parentScheduleTimes()).isEqualTo(expect);
    }

    private LocalDateTime parse(String dateStr) throws ParseException {
        return localDateTime(formatter.parse(dateStr));
    }

    @Test
    public void when_cron_unit_is_greater_than_days() throws ParseException {
        String parentCron = "00 30 09 10 * ?";
        String childCron = "00 */5 00-00 * * ?";

        DefaultDependencyResolver checker = builder().parentCronExpression(parentCron)
                                             .childCronExpression(childCron)
                                             .childScheduleTime(parse("2020-01-03 00:50:00"))
                                             .build();

        List<LocalDateTime> expect = asList(parse("2019-12-10 09:30:00"));

        assertThat(checker.parentScheduleTimes()).isEqualTo(expect);
    }

    @Test
    public void test() throws ParseException {
        String parentCron = "4 1 1,13 * * ?";

        System.out.println(on("~").join(CronUtils.calTimeRange(parse("2019-12-11 01:01:04"), parentCron)));
        System.out.println(on("~").join(CronUtils.calTimeRange(parse("2019-12-11 13:01:04"), parentCron)));
    }
}