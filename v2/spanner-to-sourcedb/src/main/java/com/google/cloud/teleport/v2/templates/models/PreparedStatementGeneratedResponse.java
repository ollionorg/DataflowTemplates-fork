package com.google.cloud.teleport.v2.templates.models;

import java.util.List;

public class PreparedStatementGeneratedResponse extends DMLGeneratorResponse{
    private List<PreparedStatementValueObject<?>> values;

    public PreparedStatementGeneratedResponse(String dmlStatement, List<PreparedStatementValueObject<?>> values) {
        super(dmlStatement);
        this.values = values;
    }

    public List<PreparedStatementValueObject<?>> getValues() {
        return values;
    }

    public void setValues(List<PreparedStatementValueObject<?>> values) {
        this.values = values;
    }
}
