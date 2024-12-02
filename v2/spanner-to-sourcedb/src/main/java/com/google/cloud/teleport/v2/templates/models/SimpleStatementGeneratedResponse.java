package com.google.cloud.teleport.v2.templates.models;
import java.util.List;

public class SimpleStatementGeneratedResponse implements DMLGeneratorResponse {
    private String dmlStatement;

    public SimpleStatementGeneratedResponse(String dmlStatement) {
        this.dmlStatement = dmlStatement;
    }

    @Override
    public String getDmlStatement() {
        return dmlStatement;
    }

    @Override
    public Boolean isPreparedStatement() {
        return false;
    }

    @Override
    public List<Object> getValues() {
        return List.of();
    }
}