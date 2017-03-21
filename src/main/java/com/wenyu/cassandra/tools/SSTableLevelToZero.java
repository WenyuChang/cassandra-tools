/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wenyu.cassandra.tools;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;

import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Reset level to 0 on a given set of sstables
 */
public class SSTableLevelToZero {
    /**
     * @param args a list of sstables whose metadata we are changing
     */
    public static void main(String[] args) throws IOException {
        PrintStream out = System.out;
        if (args.length < 3) {
            out.println("This command should be run with Cassandra stopped!");
            out.println("Usage: sstableleveltozero <keyspace> <columnfamily> <sstablefiles_fullpath>");
            System.exit(1);
        }

        // TODO several daemon threads will run from here.
        // So we have to explicitly call System.exit.
        try {
            // load keyspace descriptions.
            DatabaseDescriptor.loadSchemas();

            String keyspaceName = args[0];
            String columnfamily = args[1];
            String sstables = args[2];

            Set<String> sstableSet = new HashSet<>();
            for (String sstable : sstables.split(",")) {
                sstable = sstable.trim();
                if (sstable.length() == 0) {
                    continue;
                }
                sstableSet.add(sstable);
            }

            // validate columnfamily
            if (Schema.instance.getCFMetaData(keyspaceName, columnfamily) == null) {
                System.err.println("ColumnFamily not found: " + keyspaceName + "/" + columnfamily);
                System.exit(1);
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