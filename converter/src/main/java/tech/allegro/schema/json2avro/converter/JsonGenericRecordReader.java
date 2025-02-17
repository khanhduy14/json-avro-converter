package tech.allegro.schema.json2avro.converter;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JsonGenericRecordReader {
    private final ObjectMapper mapper;
    private final JsonToAvroReader jsonToAvroReader;

    public JsonGenericRecordReader() {
        this(new ObjectMapper());
    }

    public JsonGenericRecordReader(ObjectMapper mapper) {
        this(mapper, new CompositeJsonToAvroReader());
    }

    public JsonGenericRecordReader(JsonToAvroReader jsonToAvroReader) {
        this(new ObjectMapper(), jsonToAvroReader);
    }

    public JsonGenericRecordReader(ObjectMapper mapper, UnknownFieldListener unknownFieldListener) {
        this(mapper, new CompositeJsonToAvroReader(Collections.emptyList(), unknownFieldListener));
    }

    public JsonGenericRecordReader(ObjectMapper mapper, JsonToAvroReader jsonToAvroReader) {
        this.mapper = mapper;
        this.jsonToAvroReader = jsonToAvroReader;
    }

    @SuppressWarnings("unchecked")
    public GenericData.Record read(byte[] data, Schema schema) {
        try {
            return read(mapper.readValue(data, Map.class), schema);
        } catch (IOException ex) {
            throw new AvroConversionException("Failed to parse json to map format.", ex);
        }
    }

    public GenericData.Record read(Map<String, Object> json, Schema schema) {
        try {
            return this.jsonToAvroReader.read(json, schema);
        } catch (AvroTypeException ex) {
            throw new AvroConversionException("Failed to convert JSON to Avro: " + ex.getMessage(), ex);
        } catch (AvroRuntimeException ex) {
            throw new AvroConversionException("Failed to convert JSON to Avro", ex);
        }
    }

    @SuppressWarnings("unchecked")
    public GenericData.Array<GenericData.Record> readList(byte[] data, Schema schema) {
        try {
            return readList(mapper.readValue(data, ArrayList.class), schema);
        } catch (IOException ex) {
            throw new AvroConversionException("Failed to parse json to map array list format", ex);
        }
    }

    public GenericData.Array<GenericData.Record> readList(List<Map<String, Object>> jsons, Schema schema) {
        try {
            return this.jsonToAvroReader.read(jsons, schema);
        } catch (AvroTypeException ex) {
            throw new AvroConversionException("Failed to convert JSON to Avro: " + ex.getMessage(), ex);
        } catch (AvroRuntimeException ex) {
            throw new AvroConversionException("Failed to convert JSON to Avro", ex);
        }
    }
}

