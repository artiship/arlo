package io.github.artiship.arlo.scheduler.manager;

public interface LeaderElectable {
    void electedLeader();

    void revokedLeadership();
}
