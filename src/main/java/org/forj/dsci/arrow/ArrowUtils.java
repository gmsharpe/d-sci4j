package org.forj.dsci.arrow;

import com.amazonaws.athena.connectors.docdb.TypeUtils;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.codec.Charsets;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

public class ArrowUtils
{
    /**
     * todo-15 credit Athena project and link to original source
     *
     * @param field
     * @param origVal
     * @return
     */
    public static Object coerce(Field field, Object origVal)
    {
        if (origVal == null) {
            return origVal;
        } else if (origVal instanceof ObjectId) {
            return origVal.toString();
        } else {
            ArrowType arrowType = field.getType();
            Types.MinorType minorType = Types.getMinorTypeForArrowType(arrowType);
            switch (minorType) {
                case VARCHAR:
                    if (origVal instanceof String) {
                        return origVal;
                    }

                    return String.valueOf(origVal);
                case FLOAT8:
                    if (origVal instanceof Integer) {
                        return (double) (Integer) origVal;
                    } else {
                        if (origVal instanceof Float) {
                            return (double) (Float) origVal;
                        }

                        return origVal;
                    }
                case FLOAT4:
                    if (origVal instanceof Integer) {
                        return (float) (Integer) origVal;
                    } else {
                        if (origVal instanceof Double) {
                            return ((Double) origVal).floatValue();
                        }

                        return origVal;
                    }
                case INT:
                    if (origVal instanceof Float) {
                        return ((Float) origVal).intValue();
                    } else {
                        if (origVal instanceof Double) {
                            return ((Double) origVal).intValue();
                        }

                        return origVal;
                    }
                case DATEMILLI:
                    if (origVal instanceof BsonTimestamp) {
                        return new Date((long) (((BsonTimestamp) origVal).getTime() * 1000));
                    }

                    return origVal;
                default:
                    return origVal;
            }
        }
    }

    /**
     * todo-15 credit Athena project and link to original source
     *
     * @param vectorSchemaRoot
     * @param allocator
     * @param listField
     * @param value
     * @param row
     */
    public static void writeConceptUnitsList(VectorSchemaRoot vectorSchemaRoot, BufferAllocator allocator, Field listField, Object value, int row)
    {
        FieldVector unitsListVector = vectorSchemaRoot.getVector(listField.getName());  // units list
        if (value != null) {
            UnionListWriter writer = ((ListVector) unitsListVector).getWriter();
            writer.setPosition(row);
            Field child = null;
            if (unitsListVector.getField().getChildren() != null && !unitsListVector.getField().getChildren().isEmpty()) {
                child = unitsListVector.getField().getChildren().get(0);
            }
            writer.startList();

            for (Object unitStructValue : (List) value) {
                if (unitStructValue != null) {
                    // ******************** WRITE STRUCT ****************************
                    writer.start();

                    for (Field nextChild : child.getChildren()) {
                        // replacing FieldResolver here.  We know it's a Document
                        Object rawVal = ((Document) unitStructValue).get(nextChild.getName());
                        Object unitFieldValue = TypeUtils.coerce(nextChild, rawVal);

                        // write struct value
                        if (unitFieldValue != null) {
                            ArrowType type = nextChild.getType();
                            try {
                                switch (Types.getMinorTypeForArrowType(type)) {
                                    case DATEDAY:
                                        LocalDate date = LocalDate.parse((String) unitFieldValue, ISO_LOCAL_DATE);
                                        writer.dateDay(nextChild.getName()).writeDateDay(Long.valueOf(date.toEpochDay()).intValue());
                                        break;
                                    case INT:
                                        if (unitFieldValue instanceof Long) {
                                            writer.integer(nextChild.getName()).writeInt(((Long) unitFieldValue).intValue());
                                        } else {
                                            writer.integer(nextChild.getName()).writeInt((Integer) unitFieldValue);
                                        }
                                        break;
                                    case BIGINT:
                                        writer.bigInt(nextChild.getName()).writeBigInt((Long) unitFieldValue);
                                        break;
                                    case VARCHAR:
                                        byte[] bytes;
                                        //ArrowBuf buf;
                                        if (unitFieldValue instanceof String) {
                                            bytes = ((String) unitFieldValue).getBytes(Charsets.UTF_8);
                                            try (ArrowBuf buf = allocator.buffer(bytes.length)) {
                                                buf.writeBytes(bytes);
                                                writer.varChar(nextChild.getName()).writeVarChar(0,
                                                                                                 (int) buf.readableBytes(),
                                                                                                 buf);
                                            }
                                        }
                                        break;
                                    default:
                                        throw new IllegalArgumentException("Unknown type " + type);
                                }
                            }
                            catch (RuntimeException e) {
                                throw new RuntimeException("Unable to write value for field " + nextChild.getName() + " using value " + value, e);
                            }
                        }
                    }
                    writer.end();
                }
            }
            writer.endList();
            ((ListVector) unitsListVector).setNotNull(row);
        }
    }

    /**
     * todo-15 credit Athena project and link to original source
     *
     * @param vectorSchemaRoot
     * @param allocator
     * @param conceptField
     * @param value
     * @param row
     */
    public static void writeConceptFieldValue(VectorSchemaRoot vectorSchemaRoot, BufferAllocator allocator, Field conceptField, Object value, int row) {
        FieldVector companyConceptFieldVector;
        companyConceptFieldVector = vectorSchemaRoot.getVector(conceptField.getName());
        if (companyConceptFieldVector != null) {
            // ******************** SET VALUE OF FIELD **********************
            try {
                if (value == null) {
                    switch (companyConceptFieldVector.getMinorType()) {
                        case DATEDAY:
                            ((DateDayVector) companyConceptFieldVector).setNull(row);
                            break;
                        case INT:
                            ((IntVector) companyConceptFieldVector).setNull(row);
                            break;
                        case VARCHAR:
                            ((VarCharVector) companyConceptFieldVector).setNull(row);
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown type " + companyConceptFieldVector.getMinorType());
                    }
                } else {
                    switch (companyConceptFieldVector.getMinorType()) {
                        case INT:
                            if (value instanceof Long) {
                                ((IntVector) companyConceptFieldVector).setSafe(row, ((Long) value).intValue());
                            } else {
                                ((IntVector) companyConceptFieldVector).setSafe(row, (Integer) value);
                            }
                            break;
                        case VARCHAR:
                            if (value instanceof Text) {
                                ((VarCharVector) companyConceptFieldVector).setSafe(row, (Text) value);
                            } else {
                                ((VarCharVector) companyConceptFieldVector).setSafe(row, value.toString().getBytes(Charsets.UTF_8));
                            }
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown type " + companyConceptFieldVector.getMinorType());
                    }
                }
            }
            catch (RuntimeException e) {
                String fieldName = companyConceptFieldVector.getField().getName();
                throw new RuntimeException("Unable to set value for field " + fieldName + " using value " + value, e);
            }
        }
    }
}
