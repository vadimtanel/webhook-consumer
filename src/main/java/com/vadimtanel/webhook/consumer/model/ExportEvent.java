package com.vadimtanel.webhook.consumer.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ExportEvent {
    String guid;
    String timeStamp;
    EventMessage eventMessage;
    List<ClientDestination> destinations;
}
