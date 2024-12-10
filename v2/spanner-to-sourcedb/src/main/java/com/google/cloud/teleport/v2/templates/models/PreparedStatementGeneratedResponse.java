package com.google.cloud.teleport.v2.templates.models;

import java.util.List;

public class PreparedStatementGeneratedResponse extends DMLGeneratorResponse{
    private List<Object> values;

    public PreparedStatementGeneratedResponse(String dmlStatement, List<Object> values) {
        super(dmlStatement);
        this.values = values;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }
}
