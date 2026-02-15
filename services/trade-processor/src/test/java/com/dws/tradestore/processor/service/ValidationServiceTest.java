package com.dws.tradestore.processor.service;

import com.dws.tradestore.processor.domain.model.Trade;
import com.dws.tradestore.processor.domain.model.TradeState;
import com.dws.tradestore.processor.domain.model.ValidationResult;
import com.dws.tradestore.processor.domain.validation.ExpiredStatusValidator;
import com.dws.tradestore.processor.domain.validation.MandatoryFieldValidator;
import com.dws.tradestore.processor.domain.validation.MaturityDateValidator;
import com.dws.tradestore.processor.domain.validation.VersionValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ValidationServiceTest {

    @Mock
    private MandatoryFieldValidator mandatoryFieldValidator;

    @Mock
    private VersionValidator versionValidator;

    @Mock
    private MaturityDateValidator maturityDateValidator;

    @Mock
    private ExpiredStatusValidator expiredStatusValidator;

    @Test
    void validate_happyPath_returnsResultAndInvokesChain() {
        ValidationService validationService = new ValidationService(
                mandatoryFieldValidator, versionValidator, maturityDateValidator, expiredStatusValidator);
        Trade trade = sampleTrade();

        when(mandatoryFieldValidator.validate(any(), any())).thenReturn(ValidationResult.success());

        ValidationResult result = validationService.validate(trade, Optional.<TradeState>empty());

        assertNotNull(result);
        verify(mandatoryFieldValidator).validate(any(), any());
    }

    @Test
    void validate_negative_validatorThrows_returnsFailureResult() {
        ValidationService validationService = new ValidationService(
                mandatoryFieldValidator, versionValidator, maturityDateValidator, expiredStatusValidator);
        Trade trade = sampleTrade();

        when(mandatoryFieldValidator.validate(any(), any())).thenThrow(new RuntimeException("chain failed"));

        ValidationResult result = validationService.validate(trade, Optional.<TradeState>empty());

        assertFalse(result.isValid());
        assertTrue(result.getErrors().contains("Error while validating incoming trade"));
    }

    private Trade sampleTrade() {
        return Trade.builder()
                .tradeId("T1")
                .version(1)
                .counterPartyId("CP1")
                .bookId("B1")
                .maturityDate(LocalDate.now().plusDays(2))
                .createdDate(LocalDate.now())
                .build();
    }
}

