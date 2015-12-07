package org.geekhub.objects;

public class Car extends Entity {
    private String name;
    private Double price;
    private Integer mileage;
    private Boolean used;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Integer getMileage() {
        return mileage;
    }

    public void setMileage(Integer mileage) {
        this.mileage = mileage;
    }

    public Boolean getUsed() {
        return used;
    }

    public void setUsed(Boolean used) {
        this.used = used;
    }

    @Override
    public String toString() {
        return "Car{" + "id = " + getId() + ", " +
                "name='" + name + '\'' +
                ", price=" + price +
                ", mileage=" + mileage +
                ", used=" + used +
                '}';
    }
}
