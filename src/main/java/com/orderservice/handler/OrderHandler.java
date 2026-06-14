package com.orderservice.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.orderservice.model.Order;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.HashMap;
import java.util.Map;

public class OrderHandler {

    private static final Gson gson = new Gson();
    private static final String TABLE_NAME = System.getenv("ORDERS_TABLE");
    private static final String QUEUE_URL = System.getenv("QUEUE_URL");
    private static final String TOPIC_ARN = System.getenv("TOPIC_ARN");

    private DynamoDbClient dynamoDb;
    private SqsClient sqs;
    private SnsClient sns;

    public OrderHandler() {
        this.dynamoDb = DynamoDbClient.builder().build();
        this.sqs = SqsClient.builder().build();
        this.sns = SnsClient.builder().build();
    }

    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("📥 Received request: " + request.getHttpMethod() + " " + request.getPath());

        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();

        try {
            String httpMethod = request.getHttpMethod();
            String path = request.getPath();

            if ("POST".equals(httpMethod) && path.equals("/orders")) {
                return createOrder(request, response, logger);
            } else if ("GET".equals(httpMethod) && path.contains("/orders/")) {
                String orderId = path.split("/")[2];
                return getOrder(orderId, response, logger);
            } else if ("GET".equals(httpMethod) && path.equals("/orders")) {
                return getAllOrders(response, logger);
            } else if ("PUT".equals(httpMethod) && path.contains("/orders/")) {
                String orderId = path.split("/")[2];
                return updateOrder(orderId, request, response, logger);
            } else if ("DELETE".equals(httpMethod) && path.contains("/orders/")) {
                String orderId = path.split("/")[2];
                return deleteOrder(orderId, response, logger);
            }

            response.setStatusCode(404);
            response.setBody(gson.toJson(new ErrorResponse("Endpoint not found")));
            return response;

        } catch (Exception e) {
            logger.log("❌ Error: " + e.getMessage());
            e.printStackTrace();
            response.setStatusCode(500);
            response.setBody(gson.toJson(new ErrorResponse("Internal server error: " + e.getMessage())));
            return response;
        }
    }

    private APIGatewayProxyResponseEvent createOrder(APIGatewayProxyRequestEvent request,
                                                      APIGatewayProxyResponseEvent response,
                                                      LambdaLogger logger) {
        try {
            Order order = Order.fromJson(request.getBody());
            String orderId = "ORD-" + System.currentTimeMillis();

            order.setOrderId(orderId);
            order.setStatus("PENDING");
            order.setTotalAmount(order.calculateTotalAmount());
            order.setCreatedAt(System.currentTimeMillis());
            order.setUpdatedAt(System.currentTimeMillis());

            // Save to DynamoDB
            PutItemRequest putRequest = PutItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .item(convertOrderToAttributeMap(order))
                    .build();

            dynamoDb.putItem(putRequest);
            logger.log("✅ Order created: " + orderId);

            // Send to SQS for processing
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(QUEUE_URL)
                    .messageBody(gson.toJson(Map.of(
                            "orderId", orderId,
                            "event", "ORDER_CREATED",
                            "totalAmount", order.getTotalAmount()
                    )))
                    .build());

            // Publish SNS notification
            sns.publish(PublishRequest.builder()
                    .topicArn(TOPIC_ARN)
                    .subject("New Order Created")
                    .message("Order " + orderId + " has been created with amount $" + order.getTotalAmount())
                    .build());

            JsonObject result = new JsonObject();
            result.addProperty("orderId", orderId);
            result.addProperty("message", "Order created successfully");
            result.add("order", gson.toJsonTree(order));

            response.setStatusCode(201);
            response.setBody(result.toString());
            response.setHeaders(Map.of("Content-Type", "application/json"));
            return response;

        } catch (Exception e) {
            logger.log("❌ Error creating order: " + e.getMessage());
            response.setStatusCode(400);
            response.setBody(gson.toJson(new ErrorResponse(e.getMessage())));
            return response;
        }
    }

    private APIGatewayProxyResponseEvent getOrder(String orderId,
                                                    APIGatewayProxyResponseEvent response,
                                                    LambdaLogger logger) {
        try {
            GetItemRequest getRequest = GetItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of("orderId", AttributeValue.builder().s(orderId).build()))
                    .build();

            GetItemResponse item = dynamoDb.getItem(getRequest);

            if (item.item().isEmpty()) {
                response.setStatusCode(404);
                response.setBody(gson.toJson(new ErrorResponse("Order not found")));
                return response;
            }

            Order order = convertAttributeMapToOrder(item.item());

            response.setStatusCode(200);
            response.setBody(gson.toJson(order));
            response.setHeaders(Map.of("Content-Type", "application/json"));
            return response;

        } catch (Exception e) {
            logger.log("❌ Error getting order: " + e.getMessage());
            response.setStatusCode(500);
            response.setBody(gson.toJson(new ErrorResponse(e.getMessage())));
            return response;
        }
    }

    private APIGatewayProxyResponseEvent getAllOrders(APIGatewayProxyResponseEvent response,
                                                       LambdaLogger logger) {
        try {
            ScanRequest scanRequest = ScanRequest.builder()
                    .tableName(TABLE_NAME)
                    .limit(100)
                    .build();

            ScanResponse scanResult = dynamoDb.scan(scanRequest);

            logger.log("📊 Found " + scanResult.count() + " orders");

            JsonObject result = new JsonObject();
            result.addProperty("totalOrders", scanResult.count());
            result.add("orders", gson.toJsonTree(scanResult.items()));

            response.setStatusCode(200);
            response.setBody(result.toString());
            response.setHeaders(Map.of("Content-Type", "application/json"));
            return response;

        } catch (Exception e) {
            logger.log("❌ Error scanning orders: " + e.getMessage());
            response.setStatusCode(500);
            response.setBody(gson.toJson(new ErrorResponse(e.getMessage())));
            return response;
        }
    }

    private APIGatewayProxyResponseEvent updateOrder(String orderId,
                                                      APIGatewayProxyRequestEvent request,
                                                      APIGatewayProxyResponseEvent response,
                                                      LambdaLogger logger) {
        try {
            Order updatedOrder = Order.fromJson(request.getBody());

            UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of("orderId", AttributeValue.builder().s(orderId).build()))
                    .updateExpression("SET #status = :status, #qty = :qty, updatedAt = :updatedAt")
                    .expressionAttributeNames(Map.of(
                            "#status", "status",
                            "#qty", "quantity"
                    ))
                    .expressionAttributeValues(Map.of(
                            ":status", AttributeValue.builder().s(updatedOrder.getStatus()).build(),
                            ":qty", AttributeValue.builder().n(updatedOrder.getQuantity().toString()).build(),
                            ":updatedAt", AttributeValue.builder().n(System.currentTimeMillis() + "").build()
                    ))
                    .build();

            dynamoDb.updateItem(updateRequest);
            logger.log("✅ Order updated: " + orderId);

            response.setStatusCode(200);
            response.setBody(gson.toJson(Map.of("message", "Order updated successfully")));
            response.setHeaders(Map.of("Content-Type", "application/json"));
            return response;

        } catch (Exception e) {
            logger.log("❌ Error updating order: " + e.getMessage());
            response.setStatusCode(500);
            response.setBody(gson.toJson(new ErrorResponse(e.getMessage())));
            return response;
        }
    }

    private APIGatewayProxyResponseEvent deleteOrder(String orderId,
                                                      APIGatewayProxyResponseEvent response,
                                                      LambdaLogger logger) {
        try {
            DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of("orderId", AttributeValue.builder().s(orderId).build()))
                    .build();

            dynamoDb.deleteItem(deleteRequest);
            logger.log("✅ Order deleted: " + orderId);

            response.setStatusCode(204);
            return response;

        } catch (Exception e) {
            logger.log("❌ Error deleting order: " + e.getMessage());
            response.setStatusCode(500);
            response.setBody(gson.toJson(new ErrorResponse(e.getMessage())));
            return response;
        }
    }

    // Helper methods
    private Map<String, AttributeValue> convertOrderToAttributeMap(Order order) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("orderId", AttributeValue.builder().s(order.getOrderId()).build());
        item.put("customerId", AttributeValue.builder().s(order.getCustomerId()).build());
        item.put("productName", AttributeValue.builder().s(order.getProductName()).build());
        item.put("quantity", AttributeValue.builder().n(order.getQuantity().toString()).build());
        item.put("price", AttributeValue.builder().n(order.getPrice().toString()).build());
        item.put("totalAmount", AttributeValue.builder().n(order.getTotalAmount().toString()).build());
        item.put("status", AttributeValue.builder().s(order.getStatus()).build());
        item.put("createdAt", AttributeValue.builder().n(order.getCreatedAt().toString()).build());
        item.put("updatedAt", AttributeValue.builder().n(order.getUpdatedAt().toString()).build());
        return item;
    }

    private Order convertAttributeMapToOrder(Map<String, AttributeValue> item) {
        return new Order(
                item.get("orderId").s(),
                item.get("customerId").s(),
                item.get("productName").s(),
                Integer.parseInt(item.get("quantity").n()),
                Double.parseDouble(item.get("price").n()),
                Double.parseDouble(item.get("totalAmount").n()),
                item.get("status").s(),
                Long.parseLong(item.get("createdAt").n()),
                Long.parseLong(item.get("updatedAt").n())
        );
    }

    // Error response class
    static class ErrorResponse {
        public String error;
        public long timestamp;

        ErrorResponse(String error) {
            this.error = error;
            this.timestamp = System.currentTimeMillis();
        }
    }
}

