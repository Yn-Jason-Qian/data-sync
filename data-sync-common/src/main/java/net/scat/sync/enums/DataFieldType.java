package net.scat.sync.enums;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public enum DataFieldType {
    INTEGER(BigInteger.class),
    FLOAT(Double.class),
    DECIMAL(BigDecimal.class),
    DATE(Date.class),
    STRING(String.class),
    OTHER(null);

    private final Class<?> clazz;

    DataFieldType(Class<?> clazz) {
        this.clazz = clazz;
    }

    public Class<?> getClazz() {
        return clazz;
    }
}
