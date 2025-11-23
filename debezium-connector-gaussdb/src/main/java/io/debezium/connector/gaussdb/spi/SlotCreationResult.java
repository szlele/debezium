/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.gaussdb.spi;

import io.debezium.common.annotation.Incubating;
import io.debezium.connector.gaussdb.connection.Lsn;

/**
 * A simple data container representing the creation of a newly created replication slot.
 */
@Incubating
public class SlotCreationResult {

    private final String slotName;
    private final Lsn walStartLsn;

    public SlotCreationResult(String name, String startLsn) {
        this.slotName = name;
        this.walStartLsn = Lsn.valueOf(startLsn);
    }

    /**
     * return the name of the created slot.
     */
    public String slotName() {
        return slotName;
    }

    public Lsn startLsn() {
        return walStartLsn;
    }
}
