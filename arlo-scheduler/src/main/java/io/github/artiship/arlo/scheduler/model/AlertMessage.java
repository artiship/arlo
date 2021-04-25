package io.github.artiship.arlo.scheduler.model;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class AlertMessage {
    private String content;
    private String subject;
    private String key;
}
