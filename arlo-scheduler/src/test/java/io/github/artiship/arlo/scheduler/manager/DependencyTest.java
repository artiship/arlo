package io.github.artiship.arlo.scheduler.manager;

import com.google.common.collect.Sets;
import io.github.artiship.arlo.scheduler.model.TaskSuccessRecord;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static io.github.artiship.arlo.model.enums.JobCycle.numberOfCycles;
import static io.github.artiship.arlo.model.enums.JobCycle.truncateUnit;
import static io.github.artiship.arlo.scheduler.model.TaskSuccessRecord.of;
import static io.github.artiship.arlo.utils.CronUtils.*;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;


@Slf4j
public class DependencyTest {

    protected NavigableSet<TaskSuccessRecord> history = new TreeSet<>();
    protected DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private TaskSuccessRecord mock(String cron, LocalDateTime scheduleTime, Long cost) {
        return of(1L, cron, scheduleTime, cost);
    }

    @Test
    public void child_day_parent_day() {
        String parentCron = "4 1 2 * * ?";
        String childCron = "1 0 3 * * ?";

        TaskSuccessRecord p1 = mock(parentCron, parse("2019-11-09 02:01:04"), 100L);
        TaskSuccessRecord p2 = mock(parentCron, parse("2019-11-10 02:01:04"), 100L);

        history.add(p1);
        history.add(p2);

        TaskSuccessRecord checker = mock(childCron,
                nextScheduleTimeOf(parentCron, checkPointBase("2019-11-10 03:00:01", parentCron)),
                100L);

        TaskSuccessRecord checker2 = mock(childCron,
                nextScheduleTimeOf(parentCron, checkPointBase("2019-11-10 01:00:10", parentCron)),
                100L);

        assertThat(history.ceiling(checker)).isEqualTo(p2);
        assertThat(history.ceiling(checker2)).isEqualTo(p2);
    }

    @Test
    public void child_day_parent_hour() {
        String parentCron = "4 1 */1 * * ?";
        String childCron = "3 1 3 * * ?";

        TaskSuccessRecord p1 = mock(parentCron, parse("2019-11-09 22:01:04"), 100L);
        TaskSuccessRecord p2 = mock(parentCron, parse("2019-11-09 23:01:04"), 100L);
        TaskSuccessRecord p3 = mock(parentCron, parse("2019-11-10 00:01:04"), 100L);
        TaskSuccessRecord p4 = mock(parentCron, parse("2019-11-10 01:01:04"), 100L);
        TaskSuccessRecord p5 = mock(parentCron, parse("2019-11-10 02:01:04"), 100L);
        TaskSuccessRecord p6 = mock(parentCron, parse("2019-11-10 03:01:04"), 100L);

        history.add(p1);
        history.add(p2);
        history.add(p3);
        history.add(p4);
        history.add(p5);
        history.add(p6);

        TaskSuccessRecord checker = mock(childCron,
                nextScheduleTimeOf(parentCron, checkPointBase("2019-11-10 03:01:03", childCron)),
                100L);

        assertThat(history.ceiling(checker)).isEqualTo(p3);
    }

    @Test
    public void child_day_parent_hour_and_not_self_depended() {
        String parentCron = "4 1 */1 * * ?";
        String childCron = "3 1 3 * * ?";


        range(1, 24).mapToObj(i -> record(parentCron, "2019-11-09 " + (i < 10 ? "0" + i : i) + ":01:04"))
                    .forEach(history::add);

        LocalDateTime baseEnd = checkPointBase("2019-11-10 03:01:03", childCron);
        LocalDateTime baseStart = baseEnd.minus(intervalOf(childCron), MILLIS);

        LocalDateTime parentStart = nextScheduleTimeOf(parentCron, baseStart).plus(intervalOf(parentCron), MILLIS);
        LocalDateTime parentEnd = nextScheduleTimeOf(parentCron, baseEnd);

        log.info("child base {} ~ {}", baseStart, baseEnd);
        log.info("parent range {} ~ {}", parentStart, parentEnd);

        List<LocalDateTime> ranges = computeScheduleTimesBetween(parentCron, parentStart, parentEnd);

        ranges.forEach(System.out::println);

        System.out.println("-----------all--------");

        history.forEach(System.out::println);

        System.out.println("---------success----------");

        NavigableSet<TaskSuccessRecord> taskSuccessRecords = history.subSet(record(childCron, ranges.get(0)
                                                                                                    .format(dateTimeFormatter)), true,
                record(childCron, ranges.get(ranges.size() - 1)
                                        .format(dateTimeFormatter)), true);


        taskSuccessRecords.forEach(System.out::println);

        Sets.SetView<LocalDateTime> difference = Sets.difference(new HashSet<>(ranges), taskSuccessRecords.stream()
                                                                                                          .map(i -> i.getScheduleTime())
                                                                                                          .collect(Collectors.toSet()));

        System.out.println("---------diff----------");

        difference.forEach(System.out::println);

        assertThat(difference.toString()).isEqualTo("[2019-11-10T00:01:04]");
    }

    @Test
    public void child_day_parent_hour_1_and_13() {
        String parentCron = "4 1 1,13 * * ?";
        String childCron = "3 1 3 * * ?";

        TaskSuccessRecord p1 = mock(parentCron, parse("2019-11-09 13:01:04"), 100L);
        TaskSuccessRecord p2 = mock(parentCron, parse("2019-11-10 01:01:04"), 100L);

        history.add(p1);

        TaskSuccessRecord check_point_1 = mock(childCron,
                nextScheduleTimeOf(parentCron, checkPointBase("2019-11-10 03:01:03", childCron)),
                100L);

        assertThat(history.ceiling(check_point_1)).isNull();

        history.add(p2);

        assertThat(history.ceiling(check_point_1)).isEqualTo(p2);
    }

    @Test
    public void child_hour_parent_hour_1_and_13() {
        String parentCron = "4 1 1,13 * * ?";
        String childCron = "3 1 */1 * * ?";

        TaskSuccessRecord p1 = mock(parentCron, parse("2019-11-09 13:01:04"), 100L);

        history.add(p1);

        TaskSuccessRecord check_point_1 = mock(childCron,
                nextScheduleTimeOf(parentCron, checkPointBase("2019-11-09 13:01:03", parentCron)),
                100L);

        assertThat(history.ceiling(check_point_1)).isEqualTo(p1);

        TaskSuccessRecord check_point_2 = mock(childCron,
                nextScheduleTimeOf(parentCron, checkPointBase("2019-11-09 14:01:03", parentCron)),
                100L);

        assertThat(history.ceiling(check_point_2)).isEqualTo(p1);
    }

    private LocalDateTime checkPointBase(String scheduleTimeStr, String theGreaterCycleCron) {
        long interval = intervalOf(theGreaterCycleCron);

        ChronoUnit truncateUnit = truncateUnit(interval);
        Integer cycles = numberOfCycles(interval);

        return parse(scheduleTimeStr).truncatedTo(truncateUnit)
                                     .minus(cycles - 1, truncateUnit);
    }

    @Test
    public void child_hour_parent_minute() {
        String parentCron = "3 */5 * * * ?";
        String childCron = "4 5 */1 * * ?";

        TaskSuccessRecord p1 = mock(parentCron, parse("2019-11-10 00:50:03"), 100L);
        TaskSuccessRecord p2 = mock(parentCron, parse("2019-11-10 00:55:03"), 100L);
        TaskSuccessRecord p3 = mock(parentCron, parse("2019-11-10 01:05:03"), 100L);

        history.add(p1);
        history.add(p2);

        TaskSuccessRecord checker = mock(childCron,
                nextScheduleTimeOf(parentCron, checkPointBase("2019-11-10 01:05:04", childCron)),
                100L);

        assertThat(history.ceiling(checker)).isNull();

        history.add(p3);

        assertThat(history.ceiling(checker)).isEqualTo(p3);
    }

    @Test
    public void child_hour_parent_day() {
        String parentCron = "3 1 3 * * ?";
        String childCron = "2 1 */1 * * ?";

        TaskSuccessRecord p1 = mock(parentCron, parse("2019-11-09 03:01:03"), 100L);
        history.add(p1);

        TaskSuccessRecord check_point_1 = mock(childCron,
                nextScheduleTimeOf(parentCron, checkPointBase("2019-11-10 03:01:02", parentCron)),
                100L);

        assertThat(history.ceiling(check_point_1)).isNull();

        TaskSuccessRecord p2 = mock(parentCron, parse("2019-11-10 03:01:03"), 100L);
        history.add(p2);

        TaskSuccessRecord check_point_2 = mock(childCron,
                nextScheduleTimeOf(parentCron, checkPointBase("2019-11-10 04:01:02", parentCron)),
                100L);

        assertThat(history.ceiling(check_point_2)).isEqualTo(p2);
    }

    @After
    public void after() {
        this.history.clear();
    }

    protected LocalDateTime parse(String dateTimeStr) {
        return LocalDateTime.parse(dateTimeStr, dateTimeFormatter);
    }

    protected TaskSuccessRecord record(String cron, String dateTimeStr) {
        return mock(cron, parse(dateTimeStr), 100L);
    }

    @Test
    @Ignore
    public void tree_set_correct_search_method() {
        NavigableSet<Integer> set = new TreeSet<>();
        set.add(1);
        set.add(2);
        set.add(4);

        assertThat(set.ceiling(2)).isEqualTo(2);
        assertThat(set.higher(2)).isEqualTo(4);

        set.pollFirst();

        assertThat(set.first()).isEqualTo(2);
    }

}