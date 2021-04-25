package io.github.artiship.arlo.model.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;

import java.time.LocalDateTime;

@Data
public abstract class BaseModel {
    @Id
    private Long id;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
