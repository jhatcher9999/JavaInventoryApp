package com.hatcherdev.inventorysystem;

import com.hatcherdev.inventorysystem.DAO.InventoryDAO.StorageDestination;
import com.hatcherdev.inventorysystem.bizlogic.InventoryService;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class InventoryCLI {

    //private static final Logger logger = LoggerFactory.getLogger(InventoryCLI.class);

    public enum Feature {
        NoFeatureAssigned,
        CreateRandomInventoryRecords,
        CreateInventoryUpdateRecords,
        ProcessInventoryRecordsFromKafkaTopic
    }

    public static void main(String[] args) {

        if (args.length > 1) {
            System.out.println("Invalid number of arguments.");
            System.exit(-1);
        }

        String featureArg = args[0];
        int featureInt = -1;
        try{
            featureInt = Integer.parseInt(featureArg);
        }
        catch(Exception e){
            System.out.println("Invalid number passed for first argument -- should be 1, 2, or 3.");
            System.exit(-1);
        }

        Feature feature = Feature.NoFeatureAssigned;
        if (featureInt == 1) {
            feature = Feature.CreateRandomInventoryRecords;
        } else if (featureInt == 2) {
            feature = Feature.CreateInventoryUpdateRecords;
        } else if (featureInt == 3) {
            feature = Feature.ProcessInventoryRecordsFromKafkaTopic;
        } else {
            System.out.println("Invalid number passed for first argument -- should be 1, 2, or 3.");
            System.exit(-1);
        }

        int numberOfInventoryRecords = 10; //TODO: get from args
        StorageDestination storageDestination = StorageDestination.KAFKA; //TODO: get from args

        System.out.println("Welcome to the Inventory Update App!");
        System.out.println();

        InventoryService inventoryService = new InventoryService();

        if (feature == Feature.CreateRandomInventoryRecords) {
            inventoryService.createRandomInventoryRecords(numberOfInventoryRecords, storageDestination);
            System.out.print("Inventory record creation successful.");
        } else if (feature == Feature.CreateInventoryUpdateRecords) {
            //test running updates to existing items
            inventoryService.createInventoryUpdateRecords(numberOfInventoryRecords, storageDestination);
            System.out.print("Inventory record update successful.");
        } else if (feature == Feature.ProcessInventoryRecordsFromKafkaTopic) {
            inventoryService.processInventoryEvents(numberOfInventoryRecords);
            System.out.print("Processed inventory events successfully.");
        }
        System.out.println("  Goodbye!!");

        System.exit(0);

    }

}
