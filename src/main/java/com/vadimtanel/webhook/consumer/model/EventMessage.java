package com.vadimtanel.webhook.consumer.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class EventMessage {
    @JsonProperty("event_name")
    String eventName;
    @JsonProperty("json_message")
    Map<String, Object> jsonMessage;
}
