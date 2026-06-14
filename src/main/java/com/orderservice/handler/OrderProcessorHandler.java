package com.orderservice.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.Map;

public class OrderProcessorHandler {

    private static final Gson gson = new Gson();
    private static final String TABLE_NAME = System.getenv("ORDERS_TABLE");
    private DynamoDbClient dynamoDb;

    public OrderProcessorHandler() {
        this.dynamoDb = DynamoDbClient.builder().build();
    }

    public void handleRequest(SQSEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("🔄 Processing " + event.getRecords().size() + " SQS messages");

        for (SQSEvent.SQSMessage message : event.getRecords()) {
            try {
                JsonObject body = gson.fromJson(message.getBody(), JsonObject.class);
                String orderId = body.get("orderId").getAsString();
                String eventType = body.get("event").getAsString();

                logger.log("📨 Processing message: " + eventType + " for order: " + orderId);

                if ("ORDER_CREATED".equals(eventType)) {
                    processOrderCreated(orderId, logger);
                } else if ("ORDER_CANCELLED".equals(eventType)) {
                    processOrderCancelled(orderId, logger);
                }

                logger.log("✅ Message processed successfully");

            } catch (Exception e) {
                logger.log("❌ Error processing message: " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Failed to process message", e);
            }
        }
    }

    private void processOrderCreated(String orderId, LambdaLogger logger) {
        try {
            // Update order status to PROCESSING
            UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of("orderId", AttributeValue.builder().s(orderId).build()))
                    .updateExpression("SET #status = :status, updatedAt = :updatedAt")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(
                            ":status", AttributeValue.builder().s("PROCESSING").build(),
                            ":updatedAt", AttributeValue.builder().n(System.currentTimeMillis() + "").build()
                    ))
                    .build();

            dynamoDb.updateItem(updateRequest);
            logger.log("🔄 Order status updated to PROCESSING: " + orderId);

            // Simulate processing
            Thread.sleep(2000);

            // Update to CONFIRMED
            updateRequest = UpdateItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of("orderId", AttributeValue.builder().s(orderId).build()))
                    .updateExpression("SET #status = :status, updatedAt = :updatedAt")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(
                            ":status", AttributeValue.builder().s("CONFIRMED").build(),
                            ":updatedAt", AttributeValue.builder().n(System.currentTimeMillis() + "").build()
                    ))
                    .build();

            dynamoDb.updateItem(updateRequest);
            logger.log("✅ Order confirmed: " + orderId);

        } catch (Exception e) {
            logger.log("❌ Error processing order: " + e.getMessage());
            throw new RuntimeException("Failed to process order", e);
        }
    }

    private void processOrderCancelled(String orderId, LambdaLogger logger) {
        try {
            UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of("orderId", AttributeValue.builder().s(orderId).build()))
                    .updateExpression("SET #status = :status, updatedAt = :updatedAt")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(
                            ":status", AttributeValue.builder().s("CANCELLED").build(),
                            ":updatedAt", AttributeValue.builder().n(System.currentTimeMillis() + "").build()
                    ))
                    .build();

            dynamoDb.updateItem(updateRequest);
            logger.log("❌ Order cancelled: " + orderId);

        } catch (Exception e) {
            logger.log("❌ Error cancelling order: " + e.getMessage());
            throw new RuntimeException("Failed to cancel order", e);
        }
    }
}

