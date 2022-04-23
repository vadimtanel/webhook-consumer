package com.vadimtanel.webhook.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vadimtanel.webhook.consumer.model.ClientDestination;
import com.vadimtanel.webhook.consumer.model.EventMessage;
import com.vadimtanel.webhook.consumer.model.ExportEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumerService {
    @Value(value = "${kafka.groupId}")
    private String groupId;
    @Value(value = "${kafka.topic}")
    private String topic;

    public KafkaConsumerService() {
    }

    @KafkaListener(topics = "webhook", groupId = "tanel")
    public void listenGroupFoo(String message) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            ExportEvent event = mapper.readValue(message, ExportEvent.class);
            for (ClientDestination client: event.getDestinations()) {
                RestTemplate restTemplate = new RestTemplate();
                try {
                    EventMessage result = restTemplate.postForObject(client.getUrl(), event.getEventMessage(), EventMessage.class);
                    System.out.println("respose result: " + result);
                } catch (RestClientException ex) {
                    System.out.println("error send webhook: " + ex.getMessage());
                }
            }
        } catch (JsonProcessingException e) {
            System.out.println("can't parse this message: " + message + ". error: " + e.getMessage());

        }


        System.out.println("Received Message in group tanel: " + message);
    }
}
