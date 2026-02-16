package com.dws.tradestore.ingestion.service;

import com.dws.tradestore.ingestion.exception.KafkaPublishException;
import com.dws.tradestore.ingestion.event.RejectedTradeEvent;
import com.dws.tradestore.ingestion.event.TradeRecievedEvent;
import com.dws.tradestore.ingestion.event.enums.TransmissionChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TradeIngestionKafkaPublisherTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private DlqPublisher dlqPublisher;

    @InjectMocks
    private TradeReceivedEventKafkaPublisher kafkaPublishService;

    private TradeRecievedEvent validTrade;
    private RejectedTradeEvent rejectedTrade;

    @BeforeEach
    void setUpTrades(){
        ReflectionTestUtils.setField(kafkaPublishService, "tradesTopic","Trades_Topic");
        ReflectionTestUtils.setField(kafkaPublishService, "ingestionRejectsTopic","Ingestion_Rejects_Topic");
        ReflectionTestUtils.setField(kafkaPublishService, "retryEnabled",true);
        ReflectionTestUtils.setField(kafkaPublishService, "producerTimeoutMillis",5000L);
        ReflectionTestUtils.setField(kafkaPublishService, "maxRetries",3);
        ReflectionTestUtils.setField(kafkaPublishService, "retryBackoffBaseMillis",1L);

        validTrade = TradeRecievedEvent.builder()
                .tradeId("T1")
                .version(1)
                .counterPartyId("CP-1")
                .bookId("B1")
                .maturityDate(LocalDate.now())
                .transmissionChannel(TransmissionChannel.REST_API)
                .build();

        rejectedTrade = RejectedTradeEvent.builder()
                .tradeId("T1")
                .rejectionReason("Missing Field")
                .sourceChannel(TransmissionChannel.REST_API)
                .build();
    }

    @Test
    void testPublishTrade_Success(){
        SendResult<String,Object> sendResult = mock(SendResult.class);
        CompletableFuture<SendResult<String,Object>> future =
                CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(any(Message.class))).thenReturn(future);

        kafkaPublishService.publishTradeReceivedEvent(validTrade);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(kafkaTemplate).send(messageCaptor.capture());

        Message capturedMessage = messageCaptor.getValue();
        assertThat(capturedMessage.getPayload()).isEqualTo(validTrade);
        assertThat(capturedMessage.getHeaders().get("transmission-channel")).isEqualTo(TransmissionChannel.REST_API.name());
    }

    @Test
    void testPublishTrade_Failure(){
        CompletableFuture<SendResult<String,Object>> future =
                CompletableFuture.failedFuture(new TimeoutException("Kafka unavailable"));

        when(kafkaTemplate.send(any(Message.class))).thenReturn(future);

        assertThatThrownBy(() -> kafkaPublishService.publishTradeReceivedEvent(validTrade))
            .isInstanceOf(KafkaPublishException.class)
            .hasCauseInstanceOf(Exception.class)
            .hasRootCauseMessage("Kafka unavailable");

        verify(kafkaTemplate, atLeast(3)).send(any(Message.class));
    }

    @Test
    void testPublishIngestionRejectedEvent_WhenKafkaFails_PublishesToDlqAndThrows() {
        CompletableFuture<SendResult<String,Object>> future =
                CompletableFuture.failedFuture(new TimeoutException("Kafka unavailable"));

        when(kafkaTemplate.send(any(Message.class))).thenReturn(future);
        doNothing().when(dlqPublisher).publishRejectedTrade(any(RejectedTradeEvent.class), any(Throwable.class));

        assertThatThrownBy(() -> kafkaPublishService.publishIngestionRejectedEvent(rejectedTrade))
                .isInstanceOf(KafkaPublishException.class)
                .hasMessageContaining("Failed to publish ingestion rejected trade");

        verify(dlqPublisher).publishRejectedTrade(eq(rejectedTrade), any(Throwable.class));
    }

    @Test
    void testPublishTrade_WhenMaxRetriesIsZero_AttemptsOnceAndFails() {
        ReflectionTestUtils.setField(kafkaPublishService, "maxRetries", 0);
        CompletableFuture<SendResult<String,Object>> future =
                CompletableFuture.failedFuture(new TimeoutException("Kafka unavailable"));

        when(kafkaTemplate.send(any(Message.class))).thenReturn(future);

        assertThatThrownBy(() -> kafkaPublishService.publishTradeReceivedEvent(validTrade))
                .isInstanceOf(KafkaPublishException.class)
                .hasRootCauseMessage("Kafka unavailable");

        verify(kafkaTemplate, times(1)).send(any(Message.class));
    }

}
