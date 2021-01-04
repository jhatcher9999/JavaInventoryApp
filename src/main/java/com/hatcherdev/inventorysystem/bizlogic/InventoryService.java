package com.hatcherdev.inventorysystem.bizlogic;

import com.hatcherdev.inventorysystem.DAO.InventoryDAO;
import com.hatcherdev.inventorysystem.objects.Inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class InventoryService {

    private static final Logger logger = LoggerFactory.getLogger(InventoryService.class);

    private final int DEFAULT_STORE_NUMBER = 1;

    private InventoryDAO inventoryDAO;

    public void createRandomInventoryRecords(int numberOfInventoryRecordsToCreate, InventoryDAO.StorageDestination storageDestination) {

        inventoryDAO = new InventoryDAO(storageDestination, DEFAULT_STORE_NUMBER);

        //test inserting random(ish) values into the table
        int upperRange = numberOfInventoryRecordsToCreate;
        logger.info("Inventory inserts completed - 0 of " + Integer.toString(upperRange));
        for (int i = 0; i < upperRange; i++) {
            inventoryDAO.storeInventoryChange();
            if (i > 0 && i % 1000 == 0) {
                logger.info("Inventory inserts completed - " + Integer.toString(i) + " of " + Integer.toString(upperRange));
            }
        }

        inventoryDAO.teardown();

        logger.info("Inventory inserts completed - " + Integer.toString(upperRange) + " of " + Integer.toString(upperRange));

    }

    public void createInventoryUpdateRecords(int numberOfInventoryRecordsToCreate, InventoryDAO.StorageDestination storageDestination) {

        inventoryDAO = new InventoryDAO(storageDestination, DEFAULT_STORE_NUMBER);

        List<Inventory> inventoryItems = inventoryDAO.getInventoryRecords(numberOfInventoryRecordsToCreate);

        if (inventoryItems == null) {
            logger.info("No inventory items to select from.  Run random insert first.");
            return;
        }

        int itemCount = inventoryItems.size();
        if (itemCount < numberOfInventoryRecordsToCreate) {
            logger.info("Fewer inventory items available (" + Integer.toString(itemCount) + ") than were requested (" + Integer.toString(numberOfInventoryRecordsToCreate) + ").");
            return;
        }
        System.out.println("Inventory updates completed - 0 of " + Integer.toString(itemCount));
        for (int i = 0; i < itemCount; i++) {
            Inventory inventoryItem = inventoryItems.get(i);
            inventoryDAO.storeInventoryChange(inventoryItem, true);

            if (i > 0 && i % 1000 == 0) {
                logger.info("Inventory updates completed - " + Integer.toString(i) + " of " + Integer.toString(itemCount));
            }
        }

        inventoryDAO.teardown();

        logger.info("Inventory updates completed - " + Integer.toString(itemCount) + " of " + Integer.toString(itemCount));
    }

    public void processInventoryEvents(int numberOfInventoryRecords) {

        inventoryDAO = new InventoryDAO(InventoryDAO.StorageDestination.DATABASE, DEFAULT_STORE_NUMBER);

        inventoryDAO.processInventoryEvents(numberOfInventoryRecords);

        inventoryDAO.teardown();

        logger.info("Inventory events processed - " + Integer.toString(numberOfInventoryRecords) + " of " + Integer.toString(numberOfInventoryRecords));

    }
}
