package io.github.artiship.arlo.scheduler.manager;

import io.github.artiship.arlo.scheduler.model.AlertMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import static org.springframework.http.HttpStatus.OK;

@Service
public class AlertService {
    private static final String SUBJECT = "Arlo 告警";
    @Value("${alert.url:http://inner-data-message.1sapp.com/api}")
    private String alertApiUrl;
    @Value("${alert.bot:7338b76a-c8dd-48be-9d09-0aa5feacc7d7 }")
    private String alertBot;
    @Value("${alert.enabled:true}")
    private boolean alertEnabled;
    @Autowired
    private RestTemplate restTemplate;

    public void workerDown(String workerHost) {
        if (!alertEnabled) return;
        sendBot("worker " + workerHost + " is down.");
    }

    public boolean sendBot(String content) {
        ResponseEntity<String> response = restTemplate.postForEntity(alertApiUrl + "/group_robot_alert",
                new AlertMessage().setContent(content)
                                  .setKey(alertBot)
                                  .setSubject(SUBJECT), String.class);

        if (response != null && response.getStatusCode() == OK) {
            return true;
        }

        return false;
    }
}
