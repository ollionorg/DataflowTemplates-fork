package com.google.cloud.teleport.v2.templates.models;

import java.util.List;

public class PreparedStatementGeneratedResponse implements DMLGeneratorResponse {
    private String dmlStatement;
    private List<Object> values;

    public PreparedStatementGeneratedResponse(String dmlStatement, List<Object> values) {
        this.dmlStatement = dmlStatement;
        this.values = values;
    }

    @Override
    public String getDmlStatement() {
        return dmlStatement;
    }

    @Override
    public Boolean isPreparedStatement() {
        return true;
    }

    @Override
    public List<Object> getValues() {
        return values;
    }
}
