package com.hatcherdev.inventorysystem.objects;

public class Inventory {

    private int _storeNumber;
    private String _productSku;
    private int _inventoryCount;

    public Inventory() {

    }

    public Inventory(int storeNumber, String productSku) {
        _storeNumber = storeNumber;
        _productSku = productSku;
        _inventoryCount = 0;
    }

    public Inventory(int storeNumber, String productSku, int inventoryCount) {
        _storeNumber = storeNumber;
        _productSku = productSku;
        _inventoryCount = inventoryCount;
    }

    public int getStoreNumber() {
        return _storeNumber;
    }
    public String getProductSku() {
        return _productSku;
    }
    public int getInventoryCount() {
        return _inventoryCount;
    }

    public void setStoreNumber(int storeNumber) {
        _storeNumber = storeNumber;
    }
    public void setProductSku(String productSku) {
        _productSku = productSku;
    }
    public void setInventoryCount(int inventoryCount) {
        _inventoryCount = inventoryCount;
    }


}
