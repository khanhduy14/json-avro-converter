package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import tech.allegro.schema.json2avro.converter.JsonToAvroReader;

import java.util.Collection;
import java.util.Deque;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class ArrayConverter extends AvroTypeConverterWithStrictJavaTypeCheck<Collection> {
    private final JsonToAvroReader jsonToAvroReader;

    public ArrayConverter(JsonToAvroReader jsonToAvroReader) {
        super(Collection.class);
        this.jsonToAvroReader = jsonToAvroReader;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object convertValue(Schema.Field field, Schema schema, Collection value, Deque<String> path, boolean silently) {
        if (field != null) {
            return ((Collection<Object>) value).stream()
                    .map(item -> this.jsonToAvroReader.read(field, schema.getElementType(), item, path, false))
                    .collect(toList());
        } else {
            GenericData.Array<Object> convertedArray = new GenericData.Array<>(((Collection<Object>) value).size(), schema);
            ((Collection<Object>) value).forEach(item -> convertedArray.add(this.jsonToAvroReader.read((Map<String, Object>) item, schema.getElementType())));
            return convertedArray;
        }
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> path) {
        return schema.getType().equals(Schema.Type.ARRAY);
    }
}
