import io
import avro.schema
import avro.io
import time

schema_path = "RuleMessage.avsc"
schema = avro.schema.parse(open(schema_path).read())


def generatemessage(station, model, iodata, value, connection):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(
        {
            "station": station,
            "model": model,
            "io": iodata,
            "timestamp": int(round(time.time() * 1000)),
            "value": value,
            "connection": connection
        },
        encoder
    )
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes

bytesArray = generatemessage("ex1", "md1", 12, 2.3, True)
print(bytesArray)
