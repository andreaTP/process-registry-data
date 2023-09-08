///usr/bin/env jbang "$0" "$@" ; exit $?

//DEPS com.opencsv:opencsv:4.1

import java.util.*;
import java.io.*;
import com.opencsv.*;
import static java.lang.System.*;

public class process {

    public static void main(String... args) throws Exception {
        List<List<String>> records = new ArrayList<List<String>>();
        try (CSVReader csvReader = new CSVReader(new FileReader("registry-all.csv"));) {
            String[] values = null;
            while ((values = csvReader.readNext()) != null) {
                records.add(Arrays.asList(values));
            }
        }

        var operatorImage = "integration/service-registry-rhel8-operator";
        var kafkaImage = "integration/service-registry-kafkasql-rhel8";
        var sqlImage = "integration/service-registry-sql-rhel8";

        var customers = new HashMap<String, String>();
        var operators = new HashMap<String, Integer>();
        var clusters = new HashMap<String, Set<String>>();
        var managedSql = new HashMap<String, Integer>();
        var managedKafka = new HashMap<String, Integer>();
        var unmanagedSql = new HashMap<String, Integer>();
        var unmanagedKafka = new HashMap<String, Integer>();

        var sqlOperands = new HashMap<String, Integer>();
        var kafkaOperands = new HashMap<String, Integer>();

        var firstLine = true;
        for (var line: records) {
            // Skip header
            if (firstLine) {
                firstLine = false;
                continue;
            }
            
            var customerId = line.get(0);
            var customerName = line.get(1);
            var clusterId = line.get(2);
            var isManaged = Boolean.parseBoolean(line.get(4));
            var imageName = line.get(11);

            // Storing the customer names
            var noName = "no-name";
            if (!customerName.trim().isEmpty()) {
                customers.put(customerId, customerName.replace(",", "_"));
            } else if (!customers.containsKey(customerId)) {
                customers.put(customerId, noName);
            } else {
                // do nothing
            }

            // Counting clusters
            if (!clusters.containsKey(customerId)) {
                var clusterSet = new HashSet<String>();
                clusterSet.add(clusterId);
                clusters.put(customerId, clusterSet);
            } else {
                clusters.get(customerId).add(clusterId);
            }

            if (imageName.equals(operatorImage)) {
                // Counting number of operators per customers
                if (operators.containsKey(customerId)) {
                    operators.put(customerId, operators.get(customerId) + 1);
                } else {
                    operators.put(customerId, 1);
                }
            } else {
                if (imageName.equals(sqlImage)) {
                    if (sqlOperands.containsKey(customerId)) {
                        sqlOperands.put(customerId, sqlOperands.get(customerId) + 1);
                    } else {
                        sqlOperands.put(customerId, 1);
                    }

                    if (isManaged) {
                        if (!managedSql.containsKey(customerId)) {
                            managedSql.put(customerId, 1);
                        } else {
                            managedSql.put(customerId, managedSql.get(customerId) + 1);
                        }
                    } else {
                        if (!unmanagedSql.containsKey(customerId)) {
                            unmanagedSql.put(customerId, 1);
                        } else {
                            unmanagedSql.put(customerId, unmanagedSql.get(customerId) + 1);
                        }
                    }
                } else if (imageName.equals(kafkaImage)) {
                    if (kafkaOperands.containsKey(customerId)) {
                        kafkaOperands.put(customerId, kafkaOperands.get(customerId) + 1);
                    } else {
                        kafkaOperands.put(customerId, 1);
                    }

                    if (isManaged) {
                        if (!managedKafka.containsKey(customerId)) {
                            managedKafka.put(customerId, 1);
                        } else {
                            managedKafka.put(customerId, managedKafka.get(customerId) + 1);
                        }
                    } else {
                        if (!unmanagedKafka.containsKey(customerId)) {
                            unmanagedKafka.put(customerId, 1);
                        } else {
                            unmanagedKafka.put(customerId, unmanagedKafka.get(customerId) + 1);
                        }
                    }
                } else {
                    throw new Exception("Unknown image " + imageName);
                }
            }
        }

        var total = 0;
        var customersTotalNum = 0;
        var customersTotalUsing = 0;
        var customersWithSql = 0;
        var customersWithKafka = 0;

        var installationsNum = 0;
        var sqlInstallationsNum = 0;
        var kafkaInstallationsNum = 0;

        var maxInstallationsOnOneCustomer = 0;

        var managedSqlNum = 0;
        var managedKafkaNum = 0;
        var unmanagedSqlNum = 0;
        var unmanagedKafkaNum = 0;

        var printCsv = false;
        if (printCsv) {
            out.println("customerId,customerName,operatorsNum,sqlNum,kafkaNum,hasInstallations,hasManaged,hasUnmanaged");
        }
        for (var entry: customers.entrySet()) {
            var key = entry.getKey();
            var customerString = key + "," + entry.getValue();
            var operatorsNum = operators.getOrDefault(key, 0);
            var sqlOperandsNum = sqlOperands.getOrDefault(key, 0);
            var kafkaOperandsNum = kafkaOperands.getOrDefault(key, 0);
            var hasInstallations = sqlOperandsNum + kafkaOperandsNum > 0;
            total += operatorsNum + sqlOperandsNum + kafkaOperandsNum;

            // Aggregating data
            customersTotalNum += 1;
            if (hasInstallations) {
                customersTotalUsing += 1;
            }
            var totalInstallations = sqlOperandsNum + kafkaOperandsNum;
            installationsNum += totalInstallations;
            sqlInstallationsNum += sqlOperandsNum;
            kafkaInstallationsNum += kafkaOperandsNum;
            if (sqlOperandsNum > 0) {
                customersWithSql += 1;
            }
            if (kafkaOperandsNum > 0) {
                customersWithKafka += 1;
            }

            if (maxInstallationsOnOneCustomer < totalInstallations) {
                maxInstallationsOnOneCustomer = totalInstallations;
            }

            var hasManaged = (managedSql.getOrDefault(key, 0) + managedKafka.getOrDefault(key, 0)) > 0;
            var hasUnmanaged = (unmanagedSql.getOrDefault(key, 0) + unmanagedKafka.getOrDefault(key, 0)) > 0;
            managedSqlNum += managedSql.getOrDefault(key, 0);
            managedKafkaNum += managedKafka.getOrDefault(key, 0);
            unmanagedSqlNum += unmanagedSql.getOrDefault(key, 0);
            unmanagedKafkaNum += unmanagedKafka.getOrDefault(key, 0);

            // printing to emit a CSV
            if (printCsv) {
                out.println(customerString + "," + operatorsNum + "," + sqlOperandsNum + "," + kafkaOperandsNum + "," + hasInstallations + "," + hasManaged + "," + hasUnmanaged);
            }
        }

        // Sanity check that we are processing all the lines
        if ((total + 1) != records.size()) {
            throw new RuntimeException("Missing entries!");
        }
        if (installationsNum != (sqlInstallationsNum + kafkaInstallationsNum)) {
            throw new RuntimeException("Installations number mismatch");
        }
        if ((managedSqlNum + unmanagedSqlNum) != sqlInstallationsNum) {
            throw new RuntimeException("SQL installations number mismatch");
        }
        if ((managedKafkaNum + unmanagedKafkaNum) != kafkaInstallationsNum) {
            throw new RuntimeException("Kafka installations number mismatch");
        }
        
        // Printing aggregated results
        var printAggregatedResult = true;
        if (printAggregatedResult) {
            out.println("Title,AbsoluteNum,Percentage");
            out.println();

            out.println("CUSTOMERS," + customersTotalNum);
            out.println("CUSTOMERS WITH AT LEAST ONE INSTALLATION," + customersTotalUsing + "," + ((customersTotalUsing * 100) / customersTotalNum) + "%");
            out.println();
            out.println("CUSTOMERS WITH SQL INSTALLATION," + customersWithSql + "," + ((customersWithSql * 100) / customersTotalUsing) + "%");
            out.println("CUSTOMERS WITH KAFKA INSTALLATION," + customersWithKafka + "," + ((customersWithKafka * 100) / customersTotalUsing) + "%");
            var bothInstallations = (customersWithKafka + customersWithSql - customersTotalUsing);
            out.println("CUSTOMERS WITH BOTH INSTALLATION," + bothInstallations + "," + ((bothInstallations * 100) / customersTotalUsing) + "%");
            out.println();
            out.println("INSTALLATIONS," + installationsNum);
            out.println("SQL INSTALLATIONS," + sqlInstallationsNum + "," + ((sqlInstallationsNum * 100) / installationsNum) + "%");
            out.println("KAFKA INSTALLATIONS," + kafkaInstallationsNum + "," + ((kafkaInstallationsNum * 100) / installationsNum) + "%");
            out.println();
            out.println("MANAGED SQL INSTALLATIONS," + managedSqlNum + "," + ((managedSqlNum * 100) / sqlInstallationsNum) + "%");
            out.println("UNMANAGED SQL INSTALLATIONS," + unmanagedSqlNum + "," + ((unmanagedSqlNum * 100) / sqlInstallationsNum) + "%");
            out.println();
            out.println("MANAGED KAFKA INSTALLATIONS," + managedKafkaNum + "," + ((managedKafkaNum * 100) / kafkaInstallationsNum) + "%");
            out.println("UNMANAGED KAFKA INSTALLATIONS," + unmanagedKafkaNum + "," + ((unmanagedKafkaNum * 100) / kafkaInstallationsNum) + "%");
            out.println();
            out.println("AVG INSTALLATIONS/CUSTOMERS," + (installationsNum / customersTotalUsing));
            out.println("MAX INSTALLATIONS/CUSTOMERS," + maxInstallationsOnOneCustomer);
        }
    }
}
