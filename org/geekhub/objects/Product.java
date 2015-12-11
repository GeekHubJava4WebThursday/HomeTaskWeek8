package org.geekhub.objects;

import java.time.LocalDate;

public class Product extends Entity{

    String name;
    int price;
    int amount;
    String type;
    @Ignore
    LocalDate delivered;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public LocalDate getDelivered() {
        return delivered;
    }

    public void setDelivered(LocalDate delivered) {
        this.delivered = delivered;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}

