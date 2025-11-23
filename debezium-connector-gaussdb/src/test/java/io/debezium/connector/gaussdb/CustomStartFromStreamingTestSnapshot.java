/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.gaussdb;

import io.debezium.connector.gaussdb.snapshot.AlwaysSnapshotter;

public class CustomStartFromStreamingTestSnapshot extends AlwaysSnapshotter {
    @Override
    public boolean shouldStreamEventsStartingFromSnapshot() {
        return false;
    }
}
