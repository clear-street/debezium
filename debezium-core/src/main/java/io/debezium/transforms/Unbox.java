/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


public class Unbox<R extends ConnectRecord<R>> implements Transformation<R> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public R apply(final R record) {
        logger.info("unboxing...!!");
        logger.info(record.toString());
        final Struct value = requireStruct(record.value(), "lookup");
        Object schemaObj = value.get("schemaID");

        // schemaless, so assume json and forward
        if (schemaObj == null) {
            record.headers().addString("classname", "micro.common.events");
            return record;
        }

        // lookup schema from registry
        // set record classname to the schema's name
        // deserialize record's data field into a new record
        // return new record

        return record;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        return config;
    }

    @Override
    public void close() {
    }
}
