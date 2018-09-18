package com.amazonaws.dynamodb.bootstrap;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

public class DynamoDBAttributeParser {

    private final String jsonString;

    public DynamoDBAttributeParser(String jsonString){
       this.jsonString = jsonString;
    }

    public Map<String, AttributeValue> parse() {
        //'{":name":{"S":"Amazon DynamoDB"}}';

        Map<String, AttributeValue> expressionAttributeValues =
                new HashMap<String, AttributeValue>();

        Map<String, Object> mapObject = new Gson().fromJson(jsonString, Map.class);
        for (String key : mapObject.keySet()) {
            Object typeInfoObj = mapObject.get(key);
            if (typeInfoObj instanceof Map) {
                Map<String, Object> typeInfo = (Map) typeInfoObj;
                String type = typeInfo.keySet().toArray()[0].toString();
                Object value = typeInfo.get(type);
                System.out.println("Adding [" + key + "] = [" + value + "] as " + type );
                if (type.equals("S")) {
                    expressionAttributeValues.put(key, new AttributeValue().withS(value.toString()));
                }
                if (type.equals("N")) {
                    expressionAttributeValues.put(key, new AttributeValue().withN(value.toString()));
                }
            }

        }
        return expressionAttributeValues;
    }
}
