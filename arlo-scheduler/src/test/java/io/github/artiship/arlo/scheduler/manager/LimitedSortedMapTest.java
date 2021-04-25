package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.scheduler.manager.collections.LimitedSortedByValueMap;
import io.github.artiship.arlo.scheduler.manager.dependency.DependencyBuilder;
import io.github.artiship.arlo.scheduler.model.TaskSuccessRecord;
import org.junit.Test;

import java.time.LocalDateTime;

import static com.google.common.collect.Ordering.natural;
import static io.github.artiship.arlo.scheduler.model.TaskSuccessRecord.of;
import static io.github.artiship.arlo.utils.CronUtils.calTimeRangeStr;
import static io.github.artiship.arlo.utils.Dates.localDateTime;

public class LimitedSortedMapTest {

    @Test
    public void test() {
        LimitedSortedByValueMap<String, Integer> history = new LimitedSortedByValueMap<>(natural(), 10);

        history.put("1", 1);
        history.put("2", 2);
        history.put("2", 8);
        history.put("3", 3);
        history.put("4", 4);
        history.put("5", 5);
        history.put("6", 6);
        history.put("7", 7);

        System.out.println(history);
    }

    @Test
    public void test_record() {
        LimitedSortedByValueMap<String, TaskSuccessRecord> history = new LimitedSortedByValueMap<>(natural(), 2);

        TaskSuccessRecord r1 = mock(localDateTime("2019-12-26 00:05:00"));
        TaskSuccessRecord r2 = mock(localDateTime("2019-12-26 00:10:00"));
        TaskSuccessRecord r3 = mock(localDateTime("2019-12-26 00:15:00"));
        TaskSuccessRecord r4 = mock(localDateTime("2019-12-26 00:20:00"));

        history.put(r1.getCalTimeRangeStr(), r1);
        history.put(r4.getCalTimeRangeStr(), r4);
        history.put(r3.getCalTimeRangeStr(), r3);
        history.put(r2.getCalTimeRangeStr(), r2);

        history.entrySet()
               .forEach(System.out::println);
        System.out.println(history.valueMapSize());
    }

    @Test
    public void test_not_exist() {
        LimitedSortedByValueMap<String, TaskSuccessRecord> history = new LimitedSortedByValueMap<>(natural(), 2);

        TaskSuccessRecord r1 = mock(localDateTime("2019-12-26 00:05:00"));
        TaskSuccessRecord r2 = mock(localDateTime("2019-12-26 00:10:00"));
        TaskSuccessRecord r3 = mock(localDateTime("2019-12-26 00:15:00"));
        TaskSuccessRecord r4 = mock(localDateTime("2019-12-26 00:20:00"));

        history.put(r1.getCalTimeRangeStr(), r1);
        history.put(r4.getCalTimeRangeStr(), r4);
        history.put(r3.getCalTimeRangeStr(), r3);
        history.put(r2.getCalTimeRangeStr(), r2);

        history.get("1");
    }

    @Test
    public void fix() {
        String p = "1 */1 * * * ?";
        String c = "2 */5 * * * ?";

        LocalDateTime childScheduleTime = localDateTime("2019-12-26 20:35:02");
        DependencyBuilder build = DependencyBuilder.builder()
                                                   .parentCronExpression(p)
                                                   .childCronExpression(c)
                                                   .childScheduleTime(childScheduleTime)
                                                   .build();

        build.parentScheduleTimes()
             .forEach(i -> System.out.println(calTimeRangeStr(i, p)));
    }

    @Test
    public void fix_3() {
        String p = "00 */1 00-23 * * ?";
        String c = "00 */5 00-23 * * ?";

        LocalDateTime childScheduleTime = localDateTime("2019-12-26 20:35:00");
        DependencyBuilder build = DependencyBuilder.builder()
                                                   .parentCronExpression(p)
                                                   .childCronExpression(c)
                                                   .childScheduleTime(childScheduleTime)
                                                   .build();

        build.parentScheduleTimes()
             .forEach(i -> System.out.println(calTimeRangeStr(i, p)));
    }

    @Test
    public void fix_2() {
        String parentCron = "0 0 * * * ?";
        String childCron = "0 2 0 * * ?";

        LocalDateTime childScheduleTime = localDateTime("2019-12-11 00:02:00");

        DependencyBuilder build = DependencyBuilder.builder()
                                                   .parentCronExpression(parentCron)
                                                   .childCronExpression(childCron)
                                                   .childScheduleTime(childScheduleTime)
                                                   .build();

        System.out.println(build.parentScheduleTimes());
    }

    private TaskSuccessRecord mock(LocalDateTime scheduleTime) {
        return of(1L, "00 */5 * * * ?", scheduleTime, 10L);
    }
}