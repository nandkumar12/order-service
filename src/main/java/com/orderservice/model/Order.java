package com.orderservice.model;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order implements Serializable {
    private String orderId;
    private String customerId;
    private String productName;
    private Integer quantity;
    private Double price;
    private Double totalAmount;
    private String status;
    private Long createdAt;
    private Long updatedAt;

    public static Order fromJson(String json) {
        return new Gson().fromJson(json, Order.class);
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    public Double calculateTotalAmount() {
        return (this.quantity != null && this.price != null) 
            ? this.quantity * this.price 
            : 0.0;
    }
}

