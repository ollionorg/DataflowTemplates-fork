package com.google.cloud.teleport.v2.templates.dbutils.dml;

import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class TypeHandlerTest {

    @Test
    public void convertSpannerValueJsonToBooleanType() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"isAdmin\":\"true\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "isAdmin";
        Boolean convertedValue = TypeHandler.handleCassandraBoolType(colKey, newValuesJson);
        assertTrue(convertedValue);
    }
}
