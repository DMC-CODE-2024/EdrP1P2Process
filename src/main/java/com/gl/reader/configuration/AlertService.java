package com.gl.reader.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.RestTemplate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Objects;

import static com.gl.reader.service.ProcessController.alertUrl;


@Repository
public class AlertService {
    static Logger logger = LogManager.getLogger(AlertService.class);


    private static RestTemplate  restTemplate = null;

    public static void  raiseAlert(String alertId, String alertMessage) {
        AlertDto alertDto = new AlertDto();
        alertDto.setAlertId(alertId);
        alertDto.setAlertMessage(alertMessage);
        alertDto.setAlertProcess("EDR_pre_processor");


        long start = System.currentTimeMillis();
        try {
            SimpleClientHttpRequestFactory clientHttpRequestFactory = new SimpleClientHttpRequestFactory();
            clientHttpRequestFactory.setConnectTimeout(1000);
            clientHttpRequestFactory.setReadTimeout(1000);
            restTemplate = new RestTemplate(clientHttpRequestFactory);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<AlertDto> request = new HttpEntity<AlertDto>(alertDto, headers);

            ResponseEntity<String> responseEntity = restTemplate.postForEntity(alertUrl, request, String.class);
            logger.info("Alert Sent Request:{}, TimeTaken:{} Response:{}", alertDto, responseEntity, (System.currentTimeMillis() - start));
        } catch (org.springframework.web.client.ResourceAccessException resourceAccessException) {
            logger.error("Error while Sending Alert resourceAccessException:{} Request:{}", resourceAccessException.getMessage(), alertDto, resourceAccessException);
        } catch (Exception e) {
            logger.error("Error while Sending Alert Error:{} Request:{}", e.getMessage(), alertDto, e);
        }

    }

}

class AlertDto {
    private String alertId;
    private String alertMessage;
    private String alertProcess;

    public String getAlertId() {
        return alertId;
    }

    public void setAlertId(String alertId) {
        this.alertId = alertId;
    }

    public String getAlertMessage() {
        return alertMessage;
    }

    public void setAlertMessage(String alertMessage) {
        this.alertMessage = alertMessage;
    }

    public String getAlertProcess() {
        return alertProcess;
    }

    public void setAlertProcess(String alertProcess) {
        this.alertProcess = alertProcess;
    }
}
