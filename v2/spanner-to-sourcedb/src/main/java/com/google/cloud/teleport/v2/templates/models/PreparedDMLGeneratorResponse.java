package com.google.cloud.teleport.v2.templates.models;

import java.util.List;

public class PreparedDMLGeneratorResponse extends DMLGeneratorResponse {
    private List<Object> dmlStatementValues;
    public PreparedDMLGeneratorResponse(String dmlStatement, List<Object> dmlStatementValues) {
        super(dmlStatement);
        this.dmlStatementValues = dmlStatementValues;
    }

    public List<Object> getDmlStatementValues() {
        return dmlStatementValues;
    }

    public void setDmlStatementValues(List<Object> dmlStatementValues) {
        this.dmlStatementValues = dmlStatementValues;
    }
}
