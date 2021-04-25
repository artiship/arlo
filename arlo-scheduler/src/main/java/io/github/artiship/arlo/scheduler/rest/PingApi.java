package io.github.artiship.arlo.scheduler.rest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PingApi {

    @RequestMapping(value = "/ping", method = {RequestMethod.GET, RequestMethod.HEAD})
    public ResponseEntity<String> ping() {
        return new ResponseEntity<>("OK", HttpStatus.OK);
    }
}
