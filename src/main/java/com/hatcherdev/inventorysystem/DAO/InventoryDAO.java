package com.hatcherdev.inventorysystem.DAO;

import com.hatcherdev.inventorysystem.objects.Inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

public class InventoryDAO extends DAOBase {

    private static final Logger logger = LoggerFactory.getLogger(InventoryDAO.class);
    private final Random rand = new Random();

    private final int _defaultStoreNumber;

    //pass this in or move this functionality to bizlogic class
    private final StorageDestination _storageDestination; //DB or Kafka

    //TODO: pull these in from configs
    private final String TOPIC = "inventory-events";
    private final String BOOTSTRAP_SERVERS = "localhost:9092";

    private final Producer<String, String> producer;
    private final Consumer<String, String> consumer;

    public enum StorageDestination {
        DATABASE,
        KAFKA
    }

    public InventoryDAO(StorageDestination storageDestination, int defaultStoreNumber) {
        _storageDestination = storageDestination;
        _defaultStoreNumber = defaultStoreNumber;
        producer = createProducer();
        consumer = createConsumer();
    }

    public void storeInventoryChange(Integer storeNumber, String productSKU, Integer inventoryChange) {

        if (_storageDestination == StorageDestination.DATABASE) {
            String sql = "INSERT INTO inventory ( store_no, product_sku, inventory_count ) " +
                    "VALUES ( ?, ?, ? ) " +
                    "ON CONFLICT ( store_no, product_sku ) " +
                    "DO UPDATE SET " +
                    "  inventory_count = inventory.inventory_count + excluded.inventory_count, " +
                    "  last_updated = NOW(), " +
                    "  update_count = inventory.update_count + 1;";

            int result = runSQL(sql, storeNumber.toString(), productSKU, inventoryChange.toString());
            if (result == 0) {
                logger.warn("0 records updated");
            }
        } else if (_storageDestination == StorageDestination.KAFKA) {
            String key = storeNumber.toString() + "|" + productSKU;
            String value = inventoryChange.toString();

            try {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, key, value);

                RecordMetadata metadata = producer.send(record).get();

                logger.debug("Sent record(key=%s value=%s) meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(), metadata.offset());
            }
            catch(Exception e) {
                logger.error(e.toString());
            }

            producer.flush();

        }

    }

    public void storeInventoryChange() {

        String productSKU = getRandomSKU();
        int inventoryChange = getRandomInventoryChange();

        storeInventoryChange(_defaultStoreNumber, productSKU, inventoryChange);

    }

    public void storeInventoryChange(Inventory inventory, boolean createRandomInventoryCount) {

        int storeNumber = inventory.getStoreNumber();
        String productSKU = inventory.getProductSku();
        int inventoryCount = inventory.getInventoryCount();
        if (createRandomInventoryCount) {
            inventoryCount = getRandomInventoryChange();
        }

        storeInventoryChange(storeNumber, productSKU, inventoryCount);

    }

    public List<Inventory> getInventoryRecords(int numberOfInventoryRecordsToCreate) {

        List<Inventory> inventoryList = new ArrayList<>();

        String sql = "SELECT store_no, product_sku, inventory_count FROM inventory ORDER BY random() LIMIT ?;";

        ResultSet rs = runSQLSelect(sql, Integer.toString(numberOfInventoryRecordsToCreate));

        if (rs == null) {
            return null;
        }

        try {
            while (rs.next()) {
                int store_no = rs.getInt("store_no");
                String product_sku = rs.getString("product_sku");
                int inventory_count = rs.getInt("inventory_count");
                inventoryList.add(new Inventory(store_no, product_sku, inventory_count));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return inventoryList;
    }

    public void processInventoryEvents(int numberOfInventoryRecords) {

        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

            int consumerRecordCount = consumerRecords.count();
            if (consumerRecordCount == 0) {
                break;
            }

            Iterator<ConsumerRecord<String, String>> iter = consumerRecords.iterator();
            for(int i = 0; i < consumerRecordCount; i ++) {
                if (iter.hasNext()) {
                    ConsumerRecord<String, String> record = iter.next();

                    try {
                        // vertical bar is a regex special character, so we escape it
                        String[] recordKeyFields = record.key().split("\\|");
                        int storeNumber = Integer.parseInt(recordKeyFields[0]);
                        String productSku = recordKeyFields[1];
                        int inventoryCount = Integer.parseInt(record.value());
                        Inventory inventory = new Inventory(storeNumber, productSku, inventoryCount);

                        storeInventoryChange(inventory, false);

                    }
                    catch(Exception e) {
                        logger.warn("Error processing topic message: " + record.key() + ", " + record.value() + ": " + e.toString());
                    }
                }

                if (i > 0 && i % 1000 == 0) {
                    System.out.println("Inventory events processed - " + Integer.toString(i) + " of " + Integer.toString(consumerRecordCount));
                }

            }

            consumer.commitAsync();
        }

    }

    private String getRandomSKU() {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;

        return rand.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString()
                .toUpperCase();

    }

    private int getRandomInventoryChange() {
        int minValue = 1;
        int maxValue = 5;
        return getRandomInventoryChange(minValue, maxValue);
    }

    private int getRandomInventoryChange(int min, int max) {
        //get random integer between min and max
        return rand.nextInt((max - min) + min) + min;
    }

    private Producer<String, String> createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "InventoryEventsProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    private Consumer<String, String> createConsumer() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "InventoryEventsConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singleton(TOPIC));

        return consumer;
    }

    public void teardown() {
        consumer.close();
        producer.close();

        super.teardown();
    }

}
