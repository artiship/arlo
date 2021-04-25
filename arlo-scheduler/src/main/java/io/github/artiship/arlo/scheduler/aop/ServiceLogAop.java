package io.github.artiship.arlo.scheduler.aop;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_HYPHEN;

@Slf4j
@Aspect
@Component
public class ServiceLogAop {

    @Around("execution(* io.github.artiship.arlo.scheduler.core.Service.start())")
    public void serviceStart(ProceedingJoinPoint joinPoint) throws Throwable {
        String serviceName = getServiceName(joinPoint);
        log.info("{} - Starting...", serviceName);
        joinPoint.proceed();
        log.info("{} - Start completed.", serviceName);
    }

    @Around("execution(* io.github.artiship.arlo.scheduler.core.Service.stop())")
    public void serviceStop(ProceedingJoinPoint joinPoint) throws Throwable {
        String serviceName = getServiceName(joinPoint);
        log.info("{} - Stopping...", serviceName);
        joinPoint.proceed();
        log.info("{} - Stop completed.", serviceName);
    }

    public String getServiceName(ProceedingJoinPoint joinPoint) {
        String serviceName = joinPoint.getTarget()
                                      .getClass()
                                      .getSimpleName();

        return LOWER_CAMEL.to(LOWER_HYPHEN, serviceName);
    }
}
