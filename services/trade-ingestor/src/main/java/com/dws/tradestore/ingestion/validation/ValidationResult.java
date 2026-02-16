package com.dws.tradestore.ingestion.validation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ValidationResult {
    private static final String NO_ERR_MSG = "No errors";

    private boolean valid = true;
    private String errorMessage = NO_ERR_MSG;
    private List<String> errors = new ArrayList<>();

    public void addError(String error){
        this.valid = false;
        this.errors.add(error);
    }

    public String getErrorMessage(){
        if(valid)
            return NO_ERR_MSG;
        return String.join(",", errors);
    }

}
