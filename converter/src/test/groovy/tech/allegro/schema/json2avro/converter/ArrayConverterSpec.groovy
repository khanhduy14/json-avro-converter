package tech.allegro.schema.json2avro.converter

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericData
import tech.allegro.schema.json2avro.converter.types.BytesDecimalConverter

import java.math.RoundingMode
import java.nio.ByteBuffer

class ArrayConverterSpec extends BaseConverterSpec {

    def schema = '''
            {
              "namespace": "tech.allegro.schema.json2avro.converter",
              "type": "array",
              "name": "SpecificArrayConvertTest",
              "items": {
                  "type" : "record",
                  "name" : "testSchema",
                  "fields" : [
                      {
                        "name" : "byteDecimal",
                        "type" : {
                          "type" : "bytes",
                          "logicalType" : "decimal",
                          "precision": 15, 
                          "scale": 5
                        }
                      }
                  ]
                }
            }
        '''

    def "should convert json numeric to avro decimal"() {
        given:
        def json = '''
        [{
            "byteDecimal": 123.456
        }]
        '''

        when:
        GenericData.Array<GenericData.Record> records = converter.convertToGenericDataArray(json.bytes, new Schema.Parser().parse(schema))

        then:
        new BigDecimal("123.45600") == new BigDecimal(new BigInteger(((ByteBuffer) records.get(0).get("byteDecimal")).array()), 5)
    }

}
