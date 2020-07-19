package util;

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;

public class AvroSchema {
    public static Schema getSchema() throws IOException {
        Schema avroSchema = new Schema.Parser().parse(new File("AvroTest/src/main/avro/User.avsc"));

        return avroSchema;
    }

}
