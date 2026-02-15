package com.dws.tradestore.processor.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ValidationResult {
    private boolean valid;

    @Builder.Default
    private List<String> errors = new ArrayList<>();

    public void addError(String error){
        this.errors.add(error);
        this.valid = false;
    }

    public String getErrorMessage(){
        if(valid){
            return "No Errors";
        }
        return String.join(",", errors);
    }

    public static ValidationResult success(){
        return ValidationResult.builder()
                .valid(true)
                .errors(new ArrayList<>())
                .build();
    }

    public static ValidationResult failure(String error){
        ValidationResult result = ValidationResult.builder()
                        .valid(false)
                        .errors(new ArrayList<>())
                        .build();
        result.addError(error);
        return result;
    }
}
