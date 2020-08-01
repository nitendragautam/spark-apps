package com.nitendratech.sparkgeneral.domain;

/**
 * Created by @author nitendratech on 5/17/20
 */
public class ShoppingItem {

    private String itemCode;
    private Integer itemPrice;
    private String itemDescription;

    public ShoppingItem(String itemCode, Integer itemPrice, String itemDescription) {
        this.itemCode = itemCode;
        this.itemPrice = itemPrice;
        this.itemDescription = itemDescription;
    }

    public String getItemCode() {
        return itemCode;
    }

    public void setItemCode(String itemCode) {
        this.itemCode = itemCode;
    }

    public Integer getItemPrice() {
        return itemPrice;
    }

    public void setItemPrice(Integer itemPrice) {
        this.itemPrice = itemPrice;
    }

    public String getItemDescription() {
        return itemDescription;
    }

    public void setItemDescription(String itemDescription) {
        this.itemDescription = itemDescription;
    }
}
