package com.hatcherdev.inventorysystem.DAO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.postgresql.ds.PGSimpleDataSource;
import java.sql.*;

import java.util.Random;

public class DAOBase {

    private static final Logger logger = LoggerFactory.getLogger(DAOBase.class);

    private static final int MAX_RETRY_COUNT = 3;
    private static final String RETRY_SQL_STATE = "40001";

    private final Random rand = new Random();
    private PGSimpleDataSource ds;

    private void initializeDataSource(){

        //TODO: pull these settings from config

        /*
            Setup for user:
            cockroach cert create-client app1 --certs-dir=certs --ca-key=my-safe-directory/ca.key

            create user app1;
            create database invmgmt;
            grant all on database invmgmt to app1;
        */
        String user = "app1";
        String host = "localhost";
        int port = 26257;
        String databaseName = "invmgmt";

        /*
            Java apps using JDBC expect certs to be in a DER format which is not the default format that the "cockroach cert create-client" command produces (it creates PEM format).

            You can create DER-formatted certs from the PEM-formatted with these commands:

            openssl pkcs8 -topk8 -inform PEM -outform DER -in client.app1.key -out client.app1.key.der -nocrypt
            openssl x509 -outform der -in ca.crt -out ca.crt.der
            openssl x509 -outform der -in client.app1.crt -out client.app1.crt.der
         */
        boolean useSsl = true;
        String sslMode = "verify-full";
        String sslCertPath = "/Users/jimhatcher/local_certs/client.app1.crt.der";
        String sslKeyPath = "/Users/jimhatcher/local_certs/client.app1.key.der";
        String sslRootCertPath = "/Users/jimhatcher/local_certs/ca.crt.der";


        ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{host});
        ds.setPortNumbers(new int[]{port});
        ds.setDatabaseName(databaseName);
        ds.setUser(user);
        //not setting a password (i.e. ds.setPassword("secret")) because we're authenticating with certs
        ds.setApplicationName("JavaInventoryApp");

        ds.setSsl(useSsl);
        if (useSsl) {
            ds.setSslMode(sslMode);
            ds.setSslCert(sslCertPath);
            ds.setSslKey(sslKeyPath);
            ds.setSslRootCert(sslRootCertPath);

        }

    }

    /**
     * Run SQL code in a way that automatically handles the
     * transaction retry logic so we don't have to duplicate it in
     * various places.
     *
     * @param sqlCode a String containing the SQL code you want to
     * execute.  Can have placeholders, e.g., "INSERT INTO accounts
     * (id, balance) VALUES (?, ?)".
     *
     * @param args String Varargs to fill in the SQL code's
     * placeholders.
     * @return Integer Number of rows updated, or -1 if an error is thrown.
     */
    Integer runSQL(String sqlCode, String... args) {

//        // This block is only used to emit class and method names in
//        // the program output.  It is not necessary in production
//        // code.
//        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
//        StackTraceElement elem = stacktrace[2];
//        String callerClass = elem.getClassName();
//        String callerMethod = elem.getMethodName();

        if (ds == null) {
            initializeDataSource();
        }

        int rv = 0;

        try (Connection connection = ds.getConnection()) {

            // We're managing the commit lifecycle ourselves so we can
            // automatically issue transaction retries.
            connection.setAutoCommit(false);

            int retryCount = 0;

            while (retryCount <= MAX_RETRY_COUNT) {

                if (retryCount == MAX_RETRY_COUNT) {
                    String err = String.format("hit max of %s retries, aborting", MAX_RETRY_COUNT);
                    throw new RuntimeException(err);
                }

                try (PreparedStatement pstmt = connection.prepareStatement(sqlCode)) {

                    // Loop over the args and insert them into the
                    // prepared statement based on their types.  In
                    // this simple example we classify the argument
                    // types as "integers" and "everything else"
                    // (a.k.a. strings).
                    for (int i=0; i<args.length; i++) {
                        int place = i + 1;
                        String arg = args[i];

                        try {
                            int val = Integer.parseInt(arg);
                            pstmt.setInt(place, val);
                        } catch (NumberFormatException e) {
                            pstmt.setString(place, arg);
                        }
                    }

                    if (pstmt.execute()) {
                        // We know that `pstmt.getResultSet()` will
                        // not return `null` if `pstmt.execute()` was
                        // true
                        ResultSet rs = pstmt.getResultSet();
                        ResultSetMetaData rsmeta = rs.getMetaData();
                        int colCount = rsmeta.getColumnCount();

//                        // This printed output is for debugging and/or demonstration
//                        // purposes only.  It would not be necessary in production code.
//                        System.out.printf("\n%s.%s:\n    '%s'\n", callerClass, callerMethod, pstmt);
                        logger.debug(pstmt.toString());

                        while (rs.next()) {
                            for (int i = 1; i <= colCount; i++) {
                                String name = rsmeta.getColumnName(i);
                                String type = rsmeta.getColumnTypeName(i);

                                // In this "bank account" example we know we are only handling
                                // integer values (technically 64-bit INT8s, the CockroachDB
                                // default).  This code could be made into a switch statement
                                // to handle the various SQL types needed by the application.
                                if ("int8".equals(type)) {
                                    int val = rs.getInt(name);

                                    // This printed output is for debugging and/or demonstration
                                    // purposes only.  It would not be necessary in production code.
                                    //logger.debug("    %-8s => %10s\n", name, val);
                                }
                            }
                        }
                    } else {
                        int updateCount = pstmt.getUpdateCount();
                        rv += updateCount;

//                        // This printed output is for debugging and/or demonstration
//                        // purposes only.  It would not be necessary in production code.
//                        System.out.printf("\n%s.%s:\n    '%s'\n", callerClass, callerMethod, pstmt);
                        logger.debug(pstmt.toString());
                    }

                    connection.commit();
                    break;

                } catch (SQLException e) {

                    if (RETRY_SQL_STATE.equals(e.getSQLState())) {
                        // Since this is a transaction retry error, we
                        // roll back the transaction and sleep a
                        // little before trying again.  Each time
                        // through the loop we sleep for a little
                        // longer than the last time
                        // (A.K.A. exponential backoff).
                        logger.warn(String.format("retryable exception occurred:\n    sql state = [%s]\n    message = [%s]\n    retry counter = %s\n", e.getSQLState(), e.getMessage(), retryCount));
                        connection.rollback();
                        retryCount++;
                        int sleepMillis = (int)(Math.pow(2, retryCount) * 100) + rand.nextInt(100);
                        logger.warn(String.format("Hit 40001 transaction retry error, sleeping %s milliseconds\n", sleepMillis));
                        try {
                            Thread.sleep(sleepMillis);
                        } catch (InterruptedException ignored) {
                            // Necessary to allow the Thread.sleep()
                            // above so the retry loop can continue.
                        }

                        rv = -1;
                    } else {
                        throw e;
                    }
                }
            }
        } catch (SQLException e) {
            logger.error(String.format("BasicExampleDAO.runSQL ERROR: { state => %s, cause => %s, message => %s }\n", e.getSQLState(), e.getCause(), e.getMessage()));
            rv = -1;
        }

        return rv;
    }

    /**
     * Run SQL code in a way that automatically handles the
     * transaction retry logic so we don't have to duplicate it in
     * various places.
     *
     * @param sqlCode a String containing the SQL code you want to
     * execute.  Can have placeholders, e.g., "INSERT INTO accounts
     * (id, balance) VALUES (?, ?)".
     *
     * @param args String Varargs to fill in the SQL code's
     * placeholders.
     * @return Integer Number of rows updated, or -1 if an error is thrown.
     */
    ResultSet runSQLSelect(String sqlCode, String... args) {

        if (ds == null) {
            initializeDataSource();
        }

        ResultSet rs = null;

        try (Connection connection = ds.getConnection()) {

            // We're managing the commit lifecycle ourselves so we can
            // automatically issue transaction retries.
            connection.setAutoCommit(false);

            int retryCount = 0;

            while (retryCount <= MAX_RETRY_COUNT) {

                if (retryCount == MAX_RETRY_COUNT) {
                    String err = String.format("hit max of %s retries, aborting", MAX_RETRY_COUNT);
                    throw new RuntimeException(err);
                }

                try (PreparedStatement pstmt = connection.prepareStatement(sqlCode)) {

                    // Loop over the args and insert them into the
                    // prepared statement based on their types.  In
                    // this simple example we classify the argument
                    // types as "integers" and "everything else"
                    // (a.k.a. strings).
                    for (int i=0; i<args.length; i++) {
                        int place = i + 1;
                        String arg = args[i];

                        try {
                            int val = Integer.parseInt(arg);
                            pstmt.setInt(place, val);
                        } catch (NumberFormatException e) {
                            pstmt.setString(place, arg);
                        }
                    }

                    logger.debug(pstmt.toString());
                    if (pstmt.execute()) {
                        // We know that `pstmt.getResultSet()` will
                        // not return `null` if `pstmt.execute()` was
                        // true
                        rs = pstmt.getResultSet();
                    }

                    break;

                } catch (SQLException e) {

                    if (RETRY_SQL_STATE.equals(e.getSQLState())) {
                        // Since this is a transaction retry error, we
                        // roll back the transaction and sleep a
                        // little before trying again.  Each time
                        // through the loop we sleep for a little
                        // longer than the last time
                        // (A.K.A. exponential backoff).
                        logger.warn(String.format("retryable exception occurred:\n    sql state = [%s]\n    message = [%s]\n    retry counter = %s\n", e.getSQLState(), e.getMessage(), retryCount));
                        connection.rollback();
                        retryCount++;
                        int sleepMillis = (int)(Math.pow(2, retryCount) * 100) + rand.nextInt(100);
                        logger.warn(String.format("Hit 40001 transaction retry error, sleeping %s milliseconds\n", sleepMillis));
                        try {
                            Thread.sleep(sleepMillis);
                        } catch (InterruptedException ignored) {
                            // Necessary to allow the Thread.sleep()
                            // above so the retry loop can continue.
                        }

                    } else {
                        throw e;
                    }
                }
            }
        } catch (SQLException e) {
            logger.error(String.format("runSQLSelect ERROR: { state => %s, cause => %s, message => %s }\n", e.getSQLState(), e.getCause(), e.getMessage()));
        }

        return rs;
    }

    public void teardown() {
        //nothing to tear down, but wanted to have the plumbing in place
    }

}
