
package com.wenyu.cassandra.tools;

import com.google.common.base.Throwables;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.*;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.JVMStabilityInspector;

import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;

/**
 * Reset all the sstables containing the specific partition to L0
 * Current only support connecting JMX without auth
 */
public class LCSMovePartitionToL0 {
    private final static PrintStream out = System.out;

    private static NodeProbe connect() {
        String host = "127.0.0.1";
        String port = "7199";
        NodeProbe nodeClient = null;
        try {
            nodeClient = new NodeProbe(host, parseInt(port));
        } catch (IOException e) {
            Throwable rootCause = Throwables.getRootCause(e);
            System.err.println(format("Failed to connect to '%s:%s' - %s: '%s'.", host, port, rootCause.getClass().getSimpleName(), rootCause.getMessage()));
            System.exit(1);
        }

        return nodeClient;
    }

    private static List<String> getSSTables(String ks, String cf, String key) {
        NodeProbe probe = connect();
        List<String> sstables = probe.getSSTables(ks, cf, key);

        StringBuilder log = new StringBuilder("Found " + sstables.size() + " sstables. \n[");
        for (String sstable : sstables) {
            log.append(sstable + ",");
        }
        log.append("]");
        out.println(log.toString());

        return sstables;
    }

    private static List<String> getSSTablesWithHigherLevel(List<String> orgSStables) throws IOException {
        List<String> filtered = new ArrayList<String>();
        for (String sstable : orgSStables) {
            Descriptor descriptor = Descriptor.fromFilename(sstable);
            Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.allOf(MetadataType.class));
            StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
            int level = stats.sstableLevel;
            if (level > 0) {
                filtered.add(sstable);
            }
        }

        StringBuilder log = new StringBuilder("Found " + filtered.size() + " sstables that are on higher level. \n[");
        for (String sstable : filtered) {
            log.append(sstable + ",");
        }
        log.append("]");
        out.println(log.toString());

        return filtered;
    }


    /**
     * @param args a list of sstables whose metadata we are changing
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            out.println("This command should be run with Cassandra stopped!");
            out.println("Usage: sstableleveltozero <keyspace> <columnfamily> <key>");
            System.exit(1);
        }

        // TODO several daemon threads will run from here.
        // So we have to explicitly call System.exit.
        try {
            String keyspaceName = args[0];
            String columnfamily = args[1];
            String key = args[2];

            // load keyspace descriptions.
            DatabaseDescriptor.loadSchemas();
            // validate columnfamily
            if (Schema.instance.getCFMetaData(keyspaceName, columnfamily) == null) {
                System.err.println("ColumnFamily not found: " + keyspaceName + "/" + columnfamily);
                System.exit(1);
            }

            List<String> sstables = getSSTables(keyspaceName, columnfamily, key);
            if (sstables == null || sstables.size() <= 0) {
                out.print("There is no sstables for " + keyspaceName + "/" + columnfamily + ":" + key);
                System.exit(0);
            }

            sstables = getSSTablesWithHigherLevel(sstables);
            if (sstables == null || sstables.size() <= 0) {
                out.print("There is no sstables for " + keyspaceName + "/" + columnfamily + ":" + key + " which is in higher level.");
                System.exit(0);
            }


            Set<String> sstableSet = new HashSet<>();
            for (String sstable : sstables) {
                sstable = sstable.trim();
                if (sstable.length() == 0) {
                    continue;
                }
                sstableSet.add(sstable);
            }

            Keyspace keyspace = Keyspace.openWithoutSSTables(keyspaceName);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnfamily);
            boolean foundSSTable = false;
            for (Map.Entry<Descriptor, Set<Component>> sstable : cfs.directories.sstableLister().list().entrySet()) {
                if (sstable.getValue().contains(Component.STATS)) {
                    foundSSTable = true;
                    Descriptor descriptor = sstable.getKey();

                    StatsMetadata metadata = (StatsMetadata) descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.STATS);
                    String path = descriptor.filenameFor(Component.DATA);
                    if (metadata.sstableLevel > 0 && sstableSet.contains(path)) {
                        out.println("Changing level from " + metadata.sstableLevel + " to 0 on " + descriptor.filenameFor(Component.DATA));
                        descriptor.getMetadataSerializer().mutateLevel(descriptor, 0);
                    } else if (metadata.sstableLevel <= 0 && sstableSet.contains(path)) {
                        out.println("Skipped " + descriptor.filenameFor(Component.DATA) + " since it is already on level 0");
                    }
                }
            }

            if (!foundSSTable) {
                out.println("Found no sstables, did you give the correct keyspace/columnfamily?");
            }
        } catch (Throwable t) {
            JVMStabilityInspector.inspectThrowable(t);
            t.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}