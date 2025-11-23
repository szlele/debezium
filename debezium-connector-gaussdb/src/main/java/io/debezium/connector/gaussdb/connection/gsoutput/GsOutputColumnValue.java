/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.gaussdb.connection.gsoutput;

import java.math.BigDecimal;

import io.debezium.connector.gaussdb.connection.AbstractColumnValue;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.document.Value;
import io.debezium.util.Strings;

/**
 * @author Chris Cranford
 */
class GsOutputColumnValue extends AbstractColumnValue<Value> {

    private Value value;

    GsOutputColumnValue(Value value) {
        this.value = value;
    }

    @Override
    public Value getRawValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value.isNull();
    }

    @Override
    public String asString() {
        return value.asString();
    }

    @Override
    public Boolean asBoolean() {
        if (value.isBoolean()) {
            return value.asBoolean();
        }
        else if (value.isString()) {
            return "t".equalsIgnoreCase(asString());
        }
        else {
            return null;
        }
    }

    @Override
    public Integer asInteger() {
        if (value.isNumber()) {
            return value.asInteger();
        }
        else if (value.isString()) {
            String val = asString();
            if ("null".equalsIgnoreCase(val)) {
                return null;
            }
            return Integer.valueOf(val);
        }
        else {
            return null;
        }
    }

    @Override
    public Long asLong() {
        if (value.isNumber()) {
            return value.asLong();
        }
        else if (value.isString()) {
            return Long.valueOf(asString());
        }
        else {
            return null;
        }
    }

    @Override
    public Float asFloat() {
        return value.isNumber() ? value.asFloat() : Float.valueOf(asString());
    }

    @Override
    public Double asDouble() {
        return value.isNumber() ? value.asDouble() : Double.valueOf(asString());
    }

    @Override
    public SpecialValueDecimal asDecimal() {
        if (value.isInteger()) {
            return new SpecialValueDecimal(new BigDecimal(value.asInteger()));
        }
        else if (value.isLong()) {
            return new SpecialValueDecimal(new BigDecimal(value.asLong()));
        }
        else if (value.isBigInteger()) {
            return new SpecialValueDecimal(new BigDecimal(value.asBigInteger()));
        }
        return SpecialValueDecimal.valueOf(asString());
    }

    @Override
    public byte[] asByteArray() {
        return Strings.hexStringToByteArray(asString());
    }
}
