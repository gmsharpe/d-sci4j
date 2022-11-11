package org.forj.dsci.arrow;

import com.amazonaws.athena.connector.lambda.data.*;
import com.amazonaws.athena.connectors.docdb.TypeUtils;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SimpleBlockWriter;

/*
import com.amazonaws.athena.connectors.docdb.TypeUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SimpleBlockWriter;*/

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.codec.Charsets;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDateTime;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.amazonaws.athena.connector.lambda.data.BlockUtils.EPOCH;
import static com.amazonaws.athena.connector.lambda.data.BlockUtils.UTC_ZONE_ID;

public class Concept
{
    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException
    {

        String cik = "54321";
        // CIK values are padded with 0's
        cik = IntStream.range(1, 10 - cik.length()).mapToObj(i -> "0").collect(Collectors.joining("")) + cik;

        CsvMapper mapper = new CsvMapper();

        CsvSchema schema = CsvSchema.builder()
                                    .addColumn("ticker")
                                    .addColumn("cik")
                                    .build();

        ObjectReader reader = new CsvMapper().readerForMapOf(String.class)
                                             .with(schema.withColumnSeparator('\t'));

        MappingIterator<Map<String, String>> it = reader.readValues(new File("ticker.csv"));

        List<Map<String, String>> all = it.readAll();

        Map<String, String> tickerAndCik = all.stream().filter(map -> map.get("ticker").equals("")).findFirst().orElseThrow(() -> new IllegalArgumentException("Invalid Ticker"));

        all.stream().limit(5).forEach(row -> {
            try {
                System.out.println(mapper.writeValueAsString(row));
            }
            catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });


        JsonNode jsonNode = new CsvMapper().reader()
                                     .with(schema.withColumnSeparator('\t'))
                                     .readTree(new FileInputStream(new File("ticker.csv")));

        System.out.println(jsonNode.toPrettyString());
        //breakingDownAthenaQF();
    }

    public static void basics() throws InterruptedException, IOException, URISyntaxException
    {

        try (BufferAllocator allocator = new RootAllocator();
             //VectorSchemaRoot root = VectorSchemaRoot.create(EdgarApiTableUtils.COMPANY_CONCEPT_SCHEMA(), allocator)
        ) {

            //StructVector structVector = (StructVector) root.getVector("unit");

            Document node = getCompanyConcept("https://data.sec.gov/api/xbrl/companyconcept/CIK0000320193/us-gaap/Assets.json");

            Document doc2 = Document.parse(node.toJson());

            Document doc = Document.parse(node.toJson());


            doc.remove("units");



            ArrayList<Document> units = (ArrayList)((Document)doc2.get("units")).get("USD");

            units.forEach(unit -> unit.append("type", "USD"));

            doc.append("units", units);

            // SchemaUtils.inferSchema()

            SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();



            for (String key : doc.keySet()) {

                Field newField = getArrowField(key, doc.get(key));
                Types.MinorType newType = Types.getMinorTypeForArrowType(newField.getType());
                Field curField = schemaBuilder.getField(key);
                Types.MinorType curType = (curField != null) ? Types.getMinorTypeForArrowType(curField.getType()) : null;

                if (curField == null) {
                    schemaBuilder.addField(newField);
                }
                else if (newType != curType) {
                    //TODO: currently we resolve fields with mixed types by defaulting to VARCHAR. This is _not_ ideal
                   /* logger.warn("inferSchema: Encountered a mixed-type field[{}] {} vs {}, defaulting to String.",
                                key, curType, newType);*/
                    schemaBuilder.addStringField(key);
                }
                else if (curType == Types.MinorType.LIST) {
                    schemaBuilder.addField(mergeListField(key, curField, newField));
                }
                else if (curType == Types.MinorType.STRUCT) {
                    schemaBuilder.addField(mergeStructField(key, curField, newField));
                }
            }

            final Schema schema = schemaBuilder.build();
/*
            ArrayNode units = (ArrayNode)node.get("units").get("USD");
            List<JsonNode> unitNodes = new ArrayList<>();
            units.elements().forEachRemaining(unitNodes::add);
*/
            BlockAllocator blockAllocator = new BlockAllocatorImpl();

            //int rowNum = 1;

            AtomicLong numResultRows = new AtomicLong(0);

            Block block1 = blockAllocator.createBlock(schemaBuilder.build()); //EdgarApiTableUtils.COMPANY_CONCEPT_SCHEMA());

            SimpleBlockWriter simpleBlockWriter = new SimpleBlockWriter(block1);

            simpleBlockWriter.writeRows(
                    (Block block, int rowNum) -> {
                        boolean matched = true;
                        for (Field nextField : schema.getFields()) { //EdgarApiTableUtils.COMPANY_CONCEPT_SCHEMA().getFields()) {
                            Object value = TypeUtils.coerce(nextField, doc.get(nextField.getName()));
                            Types.MinorType fieldType = Types.getMinorTypeForArrowType(nextField.getType());
                            try {
                                switch (fieldType) {
                                    case LIST:
                                    case STRUCT:
                                        matched &= block.offerComplexValue(nextField.getName(), rowNum, new DocDBFieldResolver(), value);
                                        break;
                                    default:
                                        matched &= block.offerValue(nextField.getName(), rowNum, value);
                                        break;
                                }
                                if (!matched) {
                                    // do nothing return 0;
                                }
                            }
                            catch (Exception ex) {
                                throw new RuntimeException("Error while processing field " + nextField.getName(), ex);
                            }
                        }

                        numResultRows.getAndIncrement();
                        return 1;
                    });




            //BlockUtils.setComplexValue(structVector, 0, new DocDBFieldResolver(), unitNodes);
            System.out.println(block1);
        }
    }



    public static void breakingDownAthenaQF() throws InterruptedException, IOException, URISyntaxException
    {

        try (BufferAllocator allocator = new RootAllocator(2147483647L);
             //VectorSchemaRoot root = VectorSchemaRoot.create(EdgarApiTableUtils.COMPANY_CONCEPT_SCHEMA(), allocator)
        ) {

            //StructVector structVector = (StructVector) root.getVector("unit");

            Document node = getCompanyConcept("https://data.sec.gov/api/xbrl/companyconcept/CIK0000320193/us-gaap/Assets.json");

            Document doc2 = Document.parse(node.toJson());
            Document doc = Document.parse(node.toJson());
            doc.remove("units");

            ArrayList<Document> units = (ArrayList)((Document)doc2.get("units")).get("USD");

            units.forEach(unit -> unit.append("type", "USD"));

            doc.append("units", units);

            // SchemaUtils.inferSchema()

            SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

            for (String key : doc.keySet()) {

                Field newField = getArrowField(key, doc.get(key));
                Types.MinorType newType = Types.getMinorTypeForArrowType(newField.getType());
                Field curField = schemaBuilder.getField(key);
                Types.MinorType curType = (curField != null) ? Types.getMinorTypeForArrowType(curField.getType()) : null;

                if (curField == null) {
                    schemaBuilder.addField(newField);
                }
                else if (newType != curType) {
                    //TODO: currently we resolve fields with mixed types by defaulting to VARCHAR. This is _not_ ideal
                   /* logger.warn("inferSchema: Encountered a mixed-type field[{}] {} vs {}, defaulting to String.",
                                key, curType, newType);*/
                    schemaBuilder.addStringField(key);
                }
                else if (curType == Types.MinorType.LIST) {
                    schemaBuilder.addField(mergeListField(key, curField, newField));
                }
                else if (curType == Types.MinorType.STRUCT) {
                    schemaBuilder.addField(mergeStructField(key, curField, newField));
                }
            }

            final Schema schema = schemaBuilder.build();

            System.out.println(schema.toJson());
/*
            ArrayNode units = (ArrayNode)node.get("units").get("USD");
            List<JsonNode> unitNodes = new ArrayList<>();
            units.elements().forEachRemaining(unitNodes::add);
*/

            ArrayList vectors = new ArrayList();

            Iterator var5 = schema.getFields().iterator();

            while (var5.hasNext()) {
                Field next = (Field) var5.next();
                vectors.add(next.createVector(allocator));
            }

            try (VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(schema, vectors, 0)) {
                int row = 0;
                for (Field nextField : schema.getFields()) {
                    Object value = TypeUtils.coerce(nextField, doc.get(nextField.getName()));
                    Types.MinorType fieldType = Types.getMinorTypeForArrowType(nextField.getType());
                    try {
                        switch (fieldType) {
                            case LIST:
                            case STRUCT:
                                FieldVector vector = vectorSchemaRoot.getVector(nextField.getName());
                                if (vector != null) {
                                    setComplexValue(vector, row, new DocDBFieldResolver(), value);
                                }
                                break;
                            default:
                                vector = vectorSchemaRoot.getVector(nextField.getName());
                                if (vector != null) {
                                    setValue(vector, row, value);
                                }
                                break;
                        }
                    }
                    catch (Exception ex) {
                        throw new RuntimeException("Error while processing field " + nextField.getName(), ex);
                    }
                }

                vectorSchemaRoot.setRowCount(1);
                //BlockUtils.setComplexValue(structVector, 0, new DocDBFieldResolver(), unitNodes);
                System.out.println(vectorSchemaRoot.contentToTSVString());
            }
        }
    }

    private static void setNullValue(FieldVector vector, int pos) {
        switch(vector.getMinorType()) {
            case TIMESTAMPMILLITZ:
                ((TimeStampMilliTZVector)vector).setNull(pos);
                break;
            case DATEMILLI:
                ((DateMilliVector)vector).setNull(pos);
                break;
            case DATEDAY:
                ((DateDayVector)vector).setNull(pos);
                break;
            case FLOAT8:
                ((Float8Vector)vector).setNull(pos);
                break;
            case FLOAT4:
                ((Float4Vector)vector).setNull(pos);
                break;
            case INT:
                ((IntVector)vector).setNull(pos);
                break;
            case TINYINT:
                ((TinyIntVector)vector).setNull(pos);
                break;
            case SMALLINT:
                ((SmallIntVector)vector).setNull(pos);
                break;
            case UINT1:
                ((UInt1Vector)vector).setNull(pos);
                break;
            case UINT2:
                ((UInt2Vector)vector).setNull(pos);
                break;
            case UINT4:
                ((UInt4Vector)vector).setNull(pos);
                break;
            case UINT8:
                ((UInt8Vector)vector).setNull(pos);
                break;
            case BIGINT:
                ((BigIntVector)vector).setNull(pos);
                break;
            case VARBINARY:
                ((VarBinaryVector)vector).setNull(pos);
                break;
            case DECIMAL:
                ((DecimalVector)vector).setNull(pos);
                break;
            case VARCHAR:
                ((VarCharVector)vector).setNull(pos);
                break;
            case BIT:
                ((BitVector)vector).setNull(pos);
                break;
            default:
                throw new IllegalArgumentException("Unknown type " + vector.getMinorType());
        }

    }

    public static void setValue(FieldVector vector, int pos, Object value) {
        try {
            if (value == null) {
                setNullValue(vector, pos);
            } else {
                switch(vector.getMinorType()) {
                    case TIMESTAMPMILLITZ:
                        if (value instanceof LocalDateTime) {
                            DateTimeZone dtz = ((LocalDateTime)value).getChronology().getZone();
                            long dateTimeWithZone = ((LocalDateTime)value).toDateTime(dtz).getMillis();
                            ((TimeStampMilliTZVector)vector).setSafe(pos, dateTimeWithZone);
                        }

                        long ldtInLong;
                        if (value instanceof ZonedDateTime) {
                            ldtInLong = DateTimeFormatterUtil.packDateTimeWithZone((ZonedDateTime)value);
                            ((TimeStampMilliTZVector)vector).setSafe(pos, ldtInLong);
                        } else if (value instanceof java.time.LocalDateTime) {
                            ldtInLong = DateTimeFormatterUtil.packDateTimeWithZone(((java.time.LocalDateTime)value).atZone(UTC_ZONE_ID).toInstant().toEpochMilli(),
                                                                                   UTC_ZONE_ID.getId());
                            ((TimeStampMilliTZVector)vector).setSafe(pos, ldtInLong);
                        } else if (value instanceof Date) {
                            ldtInLong = Instant.ofEpochMilli(((Date)value).getTime()).atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
                            long dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone(ldtInLong, UTC_ZONE_ID.getId());
                            ((TimeStampMilliTZVector)vector).setSafe(pos, dateTimeWithZone);
                        } else {
                            ((TimeStampMilliTZVector)vector).setSafe(pos, (Long)value);
                        }
                        break;
                    case DATEMILLI:
                        if (value instanceof Date) {
                            ((DateMilliVector)vector).setSafe(pos, ((Date)value).getTime());
                        } else if (value instanceof java.time.LocalDateTime) {
                            ((DateMilliVector)vector).setSafe(pos, ((java.time.LocalDateTime)value).atZone(UTC_ZONE_ID).toInstant().toEpochMilli());
                        } else {
                            ((DateMilliVector)vector).setSafe(pos, (Long)value);
                        }
                        break;
                    case DATEDAY:
                        if (value instanceof Date) {
                            Days days = Days.daysBetween(EPOCH, new DateTime(((Date)value).getTime()));
                            ((DateDayVector)vector).setSafe(pos, days.getDays());
                        } else if (value instanceof LocalDate) {
                            int days = (int)((LocalDate)value).toEpochDay();
                            ((DateDayVector)vector).setSafe(pos, days);
                        } else if (value instanceof Long) {
                            ((DateDayVector)vector).setSafe(pos, ((Long)value).intValue());
                        } else {
                            ((DateDayVector)vector).setSafe(pos, (Integer)value);
                        }
                        break;
                    case FLOAT8:
                        ((Float8Vector)vector).setSafe(pos, (Double)value);
                        break;
                    case FLOAT4:
                        ((Float4Vector)vector).setSafe(pos, (Float)value);
                        break;
                    case INT:
                        if (value != null && value instanceof Long) {
                            ((IntVector)vector).setSafe(pos, ((Long)value).intValue());
                        } else {
                            ((IntVector)vector).setSafe(pos, (Integer)value);
                        }
                        break;
                    case TINYINT:
                        if (value instanceof Byte) {
                            ((TinyIntVector)vector).setSafe(pos, (Byte)value);
                        } else {
                            ((TinyIntVector)vector).setSafe(pos, (Integer)value);
                        }
                        break;
                    case SMALLINT:
                        if (value instanceof Short) {
                            ((SmallIntVector)vector).setSafe(pos, (Short)value);
                        } else {
                            ((SmallIntVector)vector).setSafe(pos, (Integer)value);
                        }
                        break;
                    case UINT1:
                        ((UInt1Vector)vector).setSafe(pos, (Integer)value);
                        break;
                    case UINT2:
                        ((UInt2Vector)vector).setSafe(pos, (Integer)value);
                        break;
                    case UINT4:
                        ((UInt4Vector)vector).setSafe(pos, (Integer)value);
                        break;
                    case UINT8:
                        ((UInt8Vector)vector).setSafe(pos, (long)(Integer)value);
                        break;
                    case BIGINT:
                        ((BigIntVector)vector).setSafe(pos, (Long)value);
                        break;
                    case VARBINARY:
                        ((VarBinaryVector)vector).setSafe(pos, (byte[])value);
                        break;
                    case DECIMAL:
                        DecimalVector dVector = (DecimalVector)vector;
                        BigDecimal bdVal;
                        if (value instanceof Double) {
                            bdVal = new BigDecimal((Double)value);
                            bdVal = bdVal.setScale(dVector.getScale(), RoundingMode.HALF_UP);
                            dVector.setSafe(pos, bdVal);
                        } else {
                            bdVal = ((BigDecimal)value).setScale(dVector.getScale(), RoundingMode.HALF_UP);
                            ((DecimalVector)vector).setSafe(pos, bdVal);
                        }
                        break;
                    case VARCHAR:
                        if (value instanceof Text) {
                            ((VarCharVector)vector).setSafe(pos, (Text)value);
                        } else {
                            ((VarCharVector)vector).setSafe(pos, value.toString().getBytes(Charsets.UTF_8));
                        }
                        break;
                    case BIT:
                        if (value instanceof Integer && (Integer)value > 0) {
                            ((BitVector)vector).setSafe(pos, 1);
                        } else if (value instanceof Boolean && (Boolean)value) {
                            ((BitVector)vector).setSafe(pos, 1);
                        } else {
                            ((BitVector)vector).setSafe(pos, 0);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown type " + vector.getMinorType());
                }

            }
        } catch (RuntimeException var7) {
            String fieldName = vector != null ? vector.getField().getName() : "null_vector";
            throw new RuntimeException("Unable to set value for field " + fieldName + " using value " + value, var7);
        }
    }

    public static void setComplexValue(FieldVector vector, int pos, FieldResolver resolver, Object value) throws IOException
    {
        if (vector instanceof MapVector) {
            UnionMapWriter writer = ((MapVector)vector).getWriter();
            writer.setPosition(pos);
            writeMap(vector.getAllocator(), writer, vector.getField(), pos, value, resolver);
            writer.endMap();
        } else if (vector instanceof ListVector) {
            if (value != null) {
                UnionListWriter writer = ((ListVector)vector).getWriter();
                writer.setPosition(pos);
                writeList(vector.getAllocator(), writer, vector.getField(), pos, (List)value, resolver);
                ((ListVector)vector).setNotNull(pos);
            }
        } else {
            if (!(vector instanceof StructVector)) {
                String var10002 = vector.getClass().getSimpleName();
                throw new RuntimeException("Unsupported 'Complex' vector " + var10002 + " for field " + vector.getField().getName());
            }

            BaseWriter.StructWriter writer = ((StructVector)vector).getWriter();
            writer.setPosition(pos);
            writeStruct(vector.getAllocator(), writer, vector.getField(), pos, value, resolver);
        }

    }

    protected static void writeList(BufferAllocator allocator, FieldWriter writer, Field field, int pos, Iterable value, FieldResolver resolver) {
        if (value != null) {
            Field child = null;
            if (field.getChildren() != null && !field.getChildren().isEmpty()) {
                child = (Field)field.getChildren().get(0);
            }

            writer.startList();
            Iterator itr = value.iterator();

            while(itr.hasNext()) {
                Object val = itr.next();
                if (val != null) {
                    switch(Types.getMinorTypeForArrowType(child.getType())) {
                        case STRUCT:
                            writeStruct(allocator, writer.struct(), child, pos, val, resolver);
                            break;
                        case LIST:
                            try {
                                writeList(allocator, (FieldWriter)writer.list(), child, pos, (List)val, resolver);
                                break;
                            } catch (Exception var10) {
                                throw var10;
                            }
                        default:
                            writeListValue(writer, child.getType(), allocator, val);
                    }
                }
            }

            writer.endList();
        }
    }

    protected static void writeListValue(FieldWriter writer, ArrowType type, BufferAllocator allocator, Object value) {
        if (value != null) {
            try {
                int days;
                ArrowBuf buf;
                switch(Types.getMinorTypeForArrowType(type)) {
                    case TIMESTAMPMILLITZ:
                        long dateTimeWithZone;
                        if (value instanceof ZonedDateTime) {
                            dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone((ZonedDateTime)value);
                        } else if (value instanceof java.time.LocalDateTime) {
                            dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone(((java.time.LocalDateTime)value).atZone(UTC_ZONE_ID).toInstant().toEpochMilli(), UTC_ZONE_ID.getId());
                        } else if (value instanceof Date) {
                            long ldtInLong = Instant.ofEpochMilli(((Date)value).getTime()).atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
                            dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone(ldtInLong, UTC_ZONE_ID.getId());
                        } else {
                            dateTimeWithZone = (Long)value;
                        }

                        writer.writeTimeStampMilliTZ(dateTimeWithZone);
                    case DATEMILLI:
                        if (value instanceof Date) {
                            writer.writeDateMilli(((Date)value).getTime());
                        } else {
                            writer.writeDateMilli((Long)value);
                        }
                        break;
                    case DATEDAY:
                        if (value instanceof Date) {
                            writer.writeDateDay(Days.daysBetween(EPOCH, new DateTime(((Date)value).getTime())).getDays());
                        } else if (value instanceof LocalDate) {
                            days = (int)((LocalDate)value).toEpochDay();
                            writer.writeDateDay(days);
                        } else if (value instanceof Long) {
                            writer.writeDateDay(((Long)value).intValue());
                        } else {
                            writer.writeDateDay((Integer)value);
                        }
                        break;
                    case FLOAT8:
                        writer.float8().writeFloat8((Double)value);
                        break;
                    case FLOAT4:
                        writer.float4().writeFloat4((Float)value);
                        break;
                    case INT:
                        if (value != null && value instanceof Long) {
                            writer.integer().writeInt(((Long)value).intValue());
                        } else {
                            writer.integer().writeInt((Integer)value);
                        }
                        break;
                    case TINYINT:
                        writer.tinyInt().writeTinyInt((Byte)value);
                        break;
                    case SMALLINT:
                        writer.smallInt().writeSmallInt((Short)value);
                        break;
                    case UINT1:
                        writer.uInt1().writeUInt1((Byte)value);
                        break;
                    case UINT2:
                        writer.uInt2().writeUInt2((Character)value);
                        break;
                    case UINT4:
                        writer.uInt4().writeUInt4((Integer)value);
                        break;
                    case UINT8:
                        writer.uInt8().writeUInt8((Long)value);
                        break;
                    case BIGINT:
                        writer.bigInt().writeBigInt((Long)value);
                        break;
                    case VARBINARY:
                        if (value instanceof ArrowBuf) {

                            writer.varBinary().writeVarBinary(0, (int)((ArrowBuf)value).capacity(), (ArrowBuf)value);
                        } else if (value instanceof byte[]) {
                            byte[] bytes = (byte[])value;
                            buf = allocator.buffer((long)bytes.length);

                            try {
                                buf.writeBytes(bytes);
                                writer.varBinary().writeVarBinary(0, (int)buf.readableBytes(), buf);
                            } catch (Throwable var14) {
                                if (buf != null) {
                                    try {
                                        buf.close();
                                    } catch (Throwable var11) {
                                        var14.addSuppressed(var11);
                                    }
                                }

                                throw var14;
                            }

                            if (buf != null) {
                                buf.close();
                            }
                        }
                        break;
                    case DECIMAL:
                        days = ((ArrowType.Decimal)type).getScale();
                        if (value instanceof Double) {
                            int precision = ((ArrowType.Decimal)type).getPrecision();
                            BigDecimal bdVal = new BigDecimal((Double)value);
                            bdVal = bdVal.setScale(days, RoundingMode.HALF_UP);
                            writer.decimal().writeDecimal(bdVal);
                        } else {
                            BigDecimal scaledValue = ((BigDecimal)value).setScale(days, RoundingMode.HALF_UP);
                            writer.decimal().writeDecimal(scaledValue);
                        }
                        break;
                    case VARCHAR:
                        if (value instanceof ArrowBuf) {
                            buf = (ArrowBuf)value;
                            writer.varChar().writeVarChar(0, (int)buf.readableBytes(), buf);
                        } else {
                            ArrowBuf tempBuf;
                            byte[] bytes;
                            if (value instanceof byte[]) {
                                bytes = (byte[])value;
                                tempBuf = allocator.buffer((long)bytes.length);

                                try {
                                    tempBuf.writeBytes(bytes);
                                    writer.varChar().writeVarChar(0, (int)tempBuf.readableBytes(), tempBuf);
                                } catch (Throwable var15) {
                                    if (tempBuf != null) {
                                        try {
                                            tempBuf.close();
                                        } catch (Throwable var12) {
                                            var15.addSuppressed(var12);
                                        }
                                    }

                                    throw var15;
                                }

                                if (tempBuf != null) {
                                    tempBuf.close();
                                }
                            } else {
                                bytes = value.toString().getBytes(Charsets.UTF_8);
                                tempBuf = allocator.buffer((long)bytes.length);

                                try {
                                    tempBuf.writeBytes(bytes);
                                    writer.varChar().writeVarChar(0, (int)tempBuf.readableBytes(), tempBuf);
                                } catch (Throwable var16) {
                                    if (tempBuf != null) {
                                        try {
                                            tempBuf.close();
                                        } catch (Throwable var13) {
                                            var16.addSuppressed(var13);
                                        }
                                    }

                                    throw var16;
                                }

                                if (tempBuf != null) {
                                    tempBuf.close();
                                }
                            }
                        }
                        break;
                    case BIT:
                        if (value instanceof Integer && (Integer)value > 0) {
                            writer.bit().writeBit(1);
                        } else if (value instanceof Boolean && (Boolean)value) {
                            writer.bit().writeBit(1);
                        } else {
                            writer.bit().writeBit(0);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown type " + type);
                }

            } catch (RuntimeException var17) {
                String fieldName = writer.getField() != null ? writer.getField().getName() : "null_vector";
                throw new RuntimeException("Unable to write value for field " + fieldName + " using value " + value, var17);
            }
        }
    }

    protected static void writeStructValue(BaseWriter.StructWriter writer, Field field, BufferAllocator allocator, Object value) {
        if (value != null) {
            ArrowType type = field.getType();

            try {
                int days;
                switch(Types.getMinorTypeForArrowType(type)) {
                    case TIMESTAMPMILLITZ:
                        long dateTimeWithZone;
                        if (value instanceof ZonedDateTime) {
                            dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone((ZonedDateTime)value);
                        } else if (value instanceof java.time.LocalDateTime) {
                            dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone(((java.time.LocalDateTime)value).atZone(UTC_ZONE_ID).toInstant().toEpochMilli(), UTC_ZONE_ID.getId());
                        } else if (value instanceof Date) {
                            long ldtInLong = Instant.ofEpochMilli(((Date)value).getTime()).atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
                            dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone(ldtInLong, UTC_ZONE_ID.getId());
                        } else {
                            dateTimeWithZone = (Long)value;
                        }

                        writer.timeStampMilliTZ(field.getName()).writeTimeStampMilliTZ(dateTimeWithZone);
                    case DATEMILLI:
                        if (value instanceof Date) {
                            writer.dateMilli(field.getName()).writeDateMilli(((Date)value).getTime());
                        } else {
                            writer.dateMilli(field.getName()).writeDateMilli((Long)value);
                        }
                        break;
                    case DATEDAY:
                        if (value instanceof Date) {
                            writer.dateDay(field.getName()).writeDateDay(Days.daysBetween(EPOCH, new DateTime(((Date)value).getTime())).getDays());
                        } else if (value instanceof LocalDate) {
                            days = (int)((LocalDate)value).toEpochDay();
                            writer.dateDay(field.getName()).writeDateDay(days);
                        } else if (value instanceof Long) {
                            writer.dateDay(field.getName()).writeDateDay(((Long)value).intValue());
                        } else {
                            writer.dateDay(field.getName()).writeDateDay((Integer)value);
                        }
                        break;
                    case FLOAT8:
                        writer.float8(field.getName()).writeFloat8((Double)value);
                        break;
                    case FLOAT4:
                        writer.float4(field.getName()).writeFloat4((Float)value);
                        break;
                    case INT:
                        if (value != null && value instanceof Long) {
                            writer.integer(field.getName()).writeInt(((Long)value).intValue());
                        } else {
                            writer.integer(field.getName()).writeInt((Integer)value);
                        }
                        break;
                    case TINYINT:
                        writer.tinyInt(field.getName()).writeTinyInt((Byte)value);
                        break;
                    case SMALLINT:
                        writer.smallInt(field.getName()).writeSmallInt((Short)value);
                        break;
                    case UINT1:
                        writer.uInt1(field.getName()).writeUInt1((Byte)value);
                        break;
                    case UINT2:
                        writer.uInt2(field.getName()).writeUInt2((Character)value);
                        break;
                    case UINT4:
                        writer.uInt4(field.getName()).writeUInt4((Integer)value);
                        break;
                    case UINT8:
                        writer.uInt8(field.getName()).writeUInt8((Long)value);
                        break;
                    case BIGINT:
                        writer.bigInt(field.getName()).writeBigInt((Long)value);
                        break;
                    case VARBINARY:
                        if (value instanceof ArrowBuf) {
                            ArrowBuf buf = (ArrowBuf)value;
                            writer.varBinary(field.getName()).writeVarBinary(0, (int)buf.capacity(), buf);
                        } else if (value instanceof byte[]) {
                            byte[] bytes = (byte[])value;
                            ArrowBuf buf = allocator.buffer((long)bytes.length);

                            try {
                                buf.writeBytes(bytes);
                                writer.varBinary(field.getName()).writeVarBinary(0, (int)buf.readableBytes(), buf);
                            } catch (Throwable var16) {
                                if (buf != null) {
                                    try {
                                        buf.close();
                                    } catch (Throwable var14) {
                                        var16.addSuppressed(var14);
                                    }
                                }

                                throw var16;
                            }

                            if (buf != null) {
                                buf.close();
                            }
                        }
                        break;
                    case DECIMAL:
                        days = ((ArrowType.Decimal)type).getScale();
                        int precision = ((ArrowType.Decimal)type).getPrecision();
                        BigDecimal bdVal;
                        if (value instanceof Double) {
                            bdVal = new BigDecimal((Double)value);
                            bdVal = bdVal.setScale(days, RoundingMode.HALF_UP);
                            writer.decimal(field.getName(), days, precision).writeDecimal(bdVal);
                        } else {
                            bdVal = ((BigDecimal)value).setScale(days, RoundingMode.HALF_UP);
                            writer.decimal(field.getName(), days, precision).writeDecimal(bdVal);
                        }
                        break;
                    case VARCHAR:
                        byte[] bytes;
                        ArrowBuf buf;
                        if (value instanceof String) {
                            bytes = ((String)value).getBytes(Charsets.UTF_8);
                            buf = allocator.buffer((long)bytes.length);

                            try {
                                buf.writeBytes(bytes);
                                writer.varChar(field.getName()).writeVarChar(0, (int)buf.readableBytes(), buf);
                            } catch (Throwable var18) {
                                if (buf != null) {
                                    try {
                                        buf.close();
                                    } catch (Throwable var15) {
                                        var18.addSuppressed(var15);
                                    }
                                }

                                throw var18;
                            }

                            if (buf != null) {
                                buf.close();
                            }
                        } else if (value instanceof ArrowBuf) {
                            ArrowBuf tempBuf = (ArrowBuf)value;
                            writer.varChar(field.getName()).writeVarChar(0, (int)tempBuf.readableBytes(), tempBuf);
                        } else if (value instanceof byte[]) {
                            bytes = (byte[])value;
                            buf = allocator.buffer((long)bytes.length);

                            try {
                                buf.writeBytes(bytes);
                                writer.varChar(field.getName()).writeVarChar(0, (int)buf.readableBytes(), buf);
                            } catch (Throwable var17) {
                                if (buf != null) {
                                    try {
                                        buf.close();
                                    } catch (Throwable var13) {
                                        var17.addSuppressed(var13);
                                    }
                                }

                                throw var17;
                            }

                            if (buf != null) {
                                buf.close();
                            }
                        }
                        break;
                    case BIT:
                        if (value instanceof Integer && (Integer)value > 0) {
                            writer.bit(field.getName()).writeBit(1);
                        } else if (value instanceof Boolean && (Boolean)value) {
                            writer.bit(field.getName()).writeBit(1);
                        } else {
                            writer.bit(field.getName()).writeBit(0);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown type " + type);
                }

            } catch (RuntimeException var19) {
                throw new RuntimeException("Unable to write value for field " + field.getName() + " using value " + value, var19);
            }
        }
    }

    protected static void writeStruct(BufferAllocator allocator, BaseWriter.StructWriter writer, Field field, int pos, Object value, FieldResolver resolver) {
        if (value != null) {
            writer.start();
            Iterator var6 = field.getChildren().iterator();

            while(var6.hasNext()) {
                Field nextChild = (Field)var6.next();
                Object childValue = resolver.getFieldValue(nextChild, value);
                switch(Types.getMinorTypeForArrowType(nextChild.getType())) {
                    case STRUCT:
                        writeStruct(allocator, writer.struct(nextChild.getName()), nextChild, pos, childValue, resolver);
                        break;
                    case LIST:
                        writeList(allocator, (FieldWriter)writer.list(nextChild.getName()), nextChild, pos, (List)childValue, resolver);
                        break;
                    default:
                        writeStructValue(writer, nextChild, allocator, childValue);
                }
            }

            writer.end();
        }
    }

    protected static void writeMapValue(UnionMapWriter writer, Field field, BufferAllocator allocator, Object value) throws IOException
    {
        writer.startEntry();
        if (field.getName().equalsIgnoreCase("key")) {
            writer = writer.key();
        } else {
            if (!field.getName().equalsIgnoreCase("value")) {
                throw new IllegalStateException("Invalid Arrow Map schema: " + field);
            }
        }

        if (value != null) {
            ArrowType type = field.getType();

            try {
                int days;
                switch(Types.getMinorTypeForArrowType(type)) {
                    case TIMESTAMPMILLITZ:
                        long dateTimeWithZone;
                        if (value instanceof ZonedDateTime) {
                            dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone((ZonedDateTime)value);
                        } else if (value instanceof java.time.LocalDateTime) {
                            dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone(((java.time.LocalDateTime)value).atZone(UTC_ZONE_ID).toInstant().toEpochMilli(), UTC_ZONE_ID.getId());
                        } else if (value instanceof Date) {
                            long ldtInLong = Instant.ofEpochMilli(((Date)value).getTime()).atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
                            dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone(ldtInLong, UTC_ZONE_ID.getId());
                        } else {
                            dateTimeWithZone = (Long)value;
                        }

                        writer.timeStampMilliTZ(field.getName()).writeTimeStampMilliTZ(dateTimeWithZone);
                        break;
                    case DATEMILLI:
                        if (value instanceof Date) {
                            writer.dateMilli(field.getName()).writeDateMilli(((Date)value).getTime());
                        } else {
                            writer.dateMilli(field.getName()).writeDateMilli((Long)value);
                        }
                        break;
                    case DATEDAY:
                        if (value instanceof Date) {
                            writer.dateDay(field.getName()).writeDateDay(Days.daysBetween(EPOCH, new DateTime(((Date)value).getTime())).getDays());
                        } else if (value instanceof LocalDate) {
                            days = (int)((LocalDate)value).toEpochDay();
                            writer.dateDay(field.getName()).writeDateDay(days);
                        } else if (value instanceof Long) {
                            writer.dateDay(field.getName()).writeDateDay(((Long)value).intValue());
                        } else {
                            writer.dateDay(field.getName()).writeDateDay((Integer)value);
                        }
                        break;
                    case FLOAT8:
                        writer.float8(field.getName()).writeFloat8((Double)value);
                        break;
                    case FLOAT4:
                        writer.float4(field.getName()).writeFloat4((Float)value);
                        break;
                    case INT:
                        if (value != null && value instanceof Long) {
                            writer.integer(field.getName()).writeInt(((Long)value).intValue());
                        } else {
                            writer.integer(field.getName()).writeInt((Integer)value);
                        }
                        break;
                    case TINYINT:
                        writer.tinyInt(field.getName()).writeTinyInt((Byte)value);
                        break;
                    case SMALLINT:
                        writer.smallInt(field.getName()).writeSmallInt((Short)value);
                        break;
                    case UINT1:
                        writer.uInt1(field.getName()).writeUInt1((Byte)value);
                        break;
                    case UINT2:
                        writer.uInt2(field.getName()).writeUInt2((Character)value);
                        break;
                    case UINT4:
                        writer.uInt4(field.getName()).writeUInt4((Integer)value);
                        break;
                    case UINT8:
                        writer.uInt8(field.getName()).writeUInt8((Long)value);
                        break;
                    case BIGINT:
                        writer.bigInt(field.getName()).writeBigInt((Long)value);
                        break;
                    case VARBINARY:
                        if (value instanceof ArrowBuf) {
                            ArrowBuf buf = (ArrowBuf)value;
                            writer.varBinary(field.getName()).writeVarBinary(0, (int)buf.capacity(), buf);
                        } else if (value instanceof byte[]) {
                            byte[] bytes = (byte[])value;
                            ArrowBuf buf = allocator.buffer((long)bytes.length);

                            try {
                                buf.writeBytes(bytes);
                                writer.varBinary(field.getName()).writeVarBinary(0, (int)buf.readableBytes(), buf);
                            } catch (Throwable var18) {
                                if (buf != null) {
                                    try {
                                        buf.close();
                                    } catch (Throwable var14) {
                                        var18.addSuppressed(var14);
                                    }
                                }

                                throw var18;
                            }

                            if (buf != null) {
                                buf.close();
                            }
                        }
                        break;
                    case DECIMAL:
                        days = ((ArrowType.Decimal)type).getScale();
                        int precision = ((ArrowType.Decimal)type).getPrecision();
                        BigDecimal bdVal;
                        if (value instanceof Double) {
                            bdVal = new BigDecimal((Double)value);
                            bdVal = bdVal.setScale(days, RoundingMode.HALF_UP);
                            writer.decimal(field.getName(), days, precision).writeDecimal(bdVal);
                        } else {
                            bdVal = ((BigDecimal)value).setScale(days, RoundingMode.HALF_UP);
                            writer.decimal(field.getName(), days, precision).writeDecimal(bdVal);
                        }
                        break;
                    case VARCHAR:
                        byte[] bytes;
                        ArrowBuf buf;
                        if (value instanceof String) {
                            bytes = ((String)value).getBytes(Charsets.UTF_8);
                            buf = allocator.buffer((long)bytes.length);

                            try {
                                buf.writeBytes(bytes);
                                writer.varChar(field.getName()).writeVarChar(0, (int)buf.readableBytes(), buf);
                            } catch (Throwable var17) {
                                if (buf != null) {
                                    try {
                                        buf.close();
                                    } catch (Throwable var15) {
                                        var17.addSuppressed(var15);
                                    }
                                }

                                throw var17;
                            }

                            if (buf != null) {
                                buf.close();
                            }
                        } else if (value instanceof ArrowBuf) {
                            ArrowBuf tempBuf = (ArrowBuf)value;
                            writer.varChar(field.getName()).writeVarChar(0, (int)tempBuf.readableBytes(), tempBuf);
                        } else if (value instanceof byte[]) {
                            bytes = (byte[])value;
                            buf = allocator.buffer((long)bytes.length);

                            try {
                                buf.writeBytes(bytes);
                                writer.varChar(field.getName()).writeVarChar(0, (int)buf.readableBytes(), buf);
                            } catch (Throwable var16) {
                                if (buf != null) {
                                    try {
                                        buf.close();
                                    } catch (Throwable var13) {
                                        var16.addSuppressed(var13);
                                    }
                                }

                                throw var16;
                            }

                            if (buf != null) {
                                buf.close();
                            }
                        }
                        break;
                    case BIT:
                        if (value instanceof Integer && (Integer)value > 0) {
                            writer.bit(field.getName()).writeBit(1);
                        } else if (value instanceof Boolean && (Boolean)value) {
                            writer.bit(field.getName()).writeBit(1);
                        } else {
                            writer.bit(field.getName()).writeBit(0);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown type " + type);
                }

            } catch (RuntimeException var19) {
                throw new RuntimeException("Unable to write value for field " + field.getName() + " using value " + value, var19);
            }
        }
    }

    protected static void writeMap(BufferAllocator allocator, UnionMapWriter writer, Field field, int pos, Object value, FieldResolver resolver) throws IOException
    {
        if (value != null) {
            writer.startMap();
            List<Field> children = field.getChildren();
            if (children.size() != 1) {
                throw new IllegalStateException("Invalid Arrow Map schema: " + field);
            } else {
                Field keyValueStructField = (Field)children.get(0);
                if ("entries".equals(keyValueStructField.getName()) && keyValueStructField.getType() instanceof ArrowType.Struct) {
                    List<Field> keyValueChildren = keyValueStructField.getChildren();
                    if (keyValueChildren.size() != 2) {
                        throw new IllegalStateException("Invalid Arrow Map schema: " + field);
                    } else {
                        Field keyField = (Field)keyValueChildren.get(0);
                        Field valueField = (Field)keyValueChildren.get(1);
                        if ("key".equals(keyField.getName()) && "value".equals(valueField.getName())) {
                            Iterator var11 = keyValueChildren.iterator();

                            while(var11.hasNext()) {
                                Field nextChild = (Field)var11.next();
                                Object childValue = resolver.getFieldValue(nextChild, value);
                                switch(Types.getMinorTypeForArrowType(nextChild.getType())) {
                                    case STRUCT:
                                        writeStruct(allocator, writer.struct(nextChild.getName()), nextChild, pos, childValue, resolver);
                                        break;
                                    case LIST:
                                        writeList(allocator, (FieldWriter)writer.list(nextChild.getName()), nextChild, pos, (List)childValue, resolver);
                                        break;
                                    default:
                                        writeMapValue(writer, nextChild, allocator, childValue);
                                }
                            }

                            writer.endEntry();
                        } else {
                            throw new IllegalStateException("Invalid Arrow Map schema: " + field);
                        }
                    }
                } else {
                    throw new IllegalStateException("Invalid Arrow Map schema: " + field);
                }
            }
        }
    }

    /**
     * Infers the type of a single DocumentDB document field.
     *
     * @param key The key of the field we are attempting to infer.
     * @param value A value from the key whose type we are attempting to infer.
     * @return The Apache Arrow field definition of the inferred key/value.
     */
    private static Field getArrowField(String key, Object value)
    {
        if (value instanceof String) {
            return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        }
        else if (value instanceof Integer) {
            return new Field(key, FieldType.nullable(Types.MinorType.INT.getType()), null);
        }
        else if (value instanceof Long) {
            return new Field(key, FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        }
        else if (value instanceof Boolean) {
            return new Field(key, FieldType.nullable(Types.MinorType.BIT.getType()), null);
        }
        else if (value instanceof Float) {
            return new Field(key, FieldType.nullable(Types.MinorType.FLOAT4.getType()), null);
        }
        else if (value instanceof Double) {
            return new Field(key, FieldType.nullable(Types.MinorType.FLOAT8.getType()), null);
        }
        else if (value instanceof Date) {
            return new Field(key, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
        }
        else if (value instanceof BsonTimestamp) {
            return new Field(key, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
        }
        else if (value instanceof ObjectId) {
            return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        }
        else if (value instanceof List) {
            Field child;
            if (((List) value).isEmpty()) {
                // logger.warn("getArrowType: Encountered an empty List/Array for field[{}], defaulting to List<String> due to type erasure.", key);
                return FieldBuilder.newBuilder(key, Types.MinorType.LIST.getType()).addStringField("").build();
            }
            else {
                child = getArrowField("", ((List) value).get(0));
            }
            return new Field(key, FieldType.nullable(Types.MinorType.LIST.getType()),
                             Collections.singletonList(child));
        }
        else if (value instanceof Document) {
            List<Field> children = new ArrayList<>();
            Document doc = (Document) value;
            for (String childKey : doc.keySet()) {
                Object childVal = doc.get(childKey);
                Field child = getArrowField(childKey, childVal);
                children.add(child);
            }
            return new Field(key, FieldType.nullable(Types.MinorType.STRUCT.getType()), children);
        }

        String className = (value == null || value.getClass() == null) ? "null" : value.getClass().getName();
        //logger.warn("Unknown type[" + className + "] for field[" + key + "], defaulting to varchar.");
        return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
    }



    /**
     * Used to merge STRUCT Field into a single Field. If called with two identical STRUCTs the output is essentially
     * the same as either of the inputs.
     *
     * @param fieldName The name of the merged Field.
     * @param curParentField The current field to use as the base for the merge.
     * @param newParentField The new field to merge into the base.
     * @return The merged field.
     */
    private static Field mergeStructField(String fieldName, Field curParentField, Field newParentField)
    {
        FieldBuilder union = FieldBuilder.newBuilder(fieldName, Types.MinorType.STRUCT.getType());
        for (Field nextCur : curParentField.getChildren()) {
            union.addField(nextCur);
        }

        for (Field nextNew : newParentField.getChildren()) {
            Field curField = union.getChild(nextNew.getName());
            if (curField == null) {
                union.addField(nextNew);
                continue;
            }

            Types.MinorType newType = Types.getMinorTypeForArrowType(nextNew.getType());
            Types.MinorType curType = Types.getMinorTypeForArrowType(curField.getType());

            if (curType != newType) {
                //TODO: currently we resolve fields with mixed types by defaulting to VARCHAR. This is _not_ ideal
                //for various reasons but also because it will cause predicate odities if used in a filter.
         /*       logger.warn("mergeStructField: Encountered a mixed-type field[{}] {} vs {}, defaulting to String.",
                            nextNew.getName(), newType, curType);*/

                union.addStringField(nextNew.getName());
            }
            else if (curType == Types.MinorType.LIST) {
                union.addField(mergeListField(nextNew.getName(), curField, nextNew));
            }
            else if (curType == Types.MinorType.STRUCT) {
                union.addField(mergeStructField(nextNew.getName(), curField, nextNew));
            }
        }

        return union.build();
    }

    /**
     * Used to merge LIST Field into a single Field. If called with two identical LISTs the output is essentially
     * the same as either of the inputs.
     *
     * @param fieldName The name of the merged Field.
     * @param curParentField The current field to use as the base for the merge.
     * @param newParentField The new field to merge into the base.
     * @return The merged field.
     */
    private static Field mergeListField(String fieldName, Field curParentField, Field newParentField)
    {
        //Apache Arrow lists have a special child that holds the concrete type of the list.
        Types.MinorType newInnerType = Types.getMinorTypeForArrowType(curParentField.getChildren().get(0).getType());
        Types.MinorType curInnerType = Types.getMinorTypeForArrowType(newParentField.getChildren().get(0).getType());
        if (newInnerType == Types.MinorType.LIST && curInnerType == Types.MinorType.LIST) {
            return FieldBuilder.newBuilder(fieldName, Types.MinorType.LIST.getType())
                               .addField(mergeStructField("", curParentField.getChildren().get(0), newParentField.getChildren().get(0))).build();
        }
        else if (curInnerType != newInnerType) {
            //TODO: currently we resolve fields with mixed types by defaulting to VARCHAR. This is _not_ ideal
           /* logger.warn("mergeListField: Encountered a mixed-type list field[{}] {} vs {}, defaulting to String.",
                        fieldName, curInnerType, newInnerType);*/
            return FieldBuilder.newBuilder(fieldName, Types.MinorType.LIST.getType()).addStringField("").build();
        }

        return curParentField;
    }

    public static Document getCompanyConcept(String url) throws IOException, InterruptedException, URISyntaxException
    {
        ObjectMapper mapper = new ObjectMapper();

        HttpClient httpClient = HttpClient.newBuilder().build();
        HttpRequest httpRequest = HttpRequest.newBuilder().GET()
                                             .uri(new URI(url))
                                             .headers("User-Agent", "gmsharpe@gmail.com")
                                             .build();

        /*HttpResponse response = httpClient.send(httpRequest,
                                                HttpResponse.BodyHandlers.ofByteArray());*/

        HttpResponse response = httpClient.send(httpRequest,
                                                HttpResponse.BodyHandlers.ofString());

        /*        return mapper.readTree((byte[])response.body());*/

        return Document.parse(response.body().toString());
    }

    public static class DocDBFieldResolver implements FieldResolver
    {
        @Override
        public Object getFieldValue(Field field, Object value)
        {
            Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
            if (minorType == Types.MinorType.LIST) {
                return TypeUtils.coerce(field, ((Document) value).get(field.getName()));
            }
            else if (value instanceof Document) {
                Object rawVal = ((Document) value).get(field.getName());
                return TypeUtils.coerce(field, rawVal);
            }
            throw new RuntimeException("Expected LIST or Document type but found " + minorType);
        }
    }


}

