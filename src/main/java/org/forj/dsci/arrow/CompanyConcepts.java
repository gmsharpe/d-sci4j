package org.forj.dsci.arrow;

import com.amazonaws.athena.connectors.docdb.TypeUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.TransferPair;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.forj.dsci.arrow.ArrowUtils.writeConceptFieldValue;
import static org.forj.dsci.arrow.ArrowUtils.writeConceptUnitsList;

public class CompanyConcepts implements AutoCloseable
{
    VectorSchemaRoot root;
    BufferAllocator allocator;
    List<FieldVector> concepts;

    public CompanyConcepts() { }

    public CompanyConcepts(List<String> ciks) throws Exception
    {
        allocator = new RootAllocator(2147483647L);
        concepts =
                COMPANY_CONCEPT_SCHEMA().getFields()
                                        .stream()
                                        .map(field -> field.createVector(allocator))
                                        .collect(Collectors.toList());

        root = new VectorSchemaRoot(COMPANY_CONCEPT_SCHEMA(), concepts, 0);

        List<Document> docs = getConceptData(ciks);
        getConcept(docs);

        getConceptFor("");

    }

    public List<Document> getConceptData(final List<String> ciks)
    {
        List<Document> docs = ciks.stream().map(cik -> {
            try {
                return getCompanyConcept(cik);
            }
            catch (IOException | InterruptedException | URISyntaxException e) {
                e.printStackTrace();
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());

        return docs;
    }

    // https://www.infoq.com/articles/apache-arrow-java/
    // https://www.tabnine.com/code/java/classes/org.apache.arrow.vector.complex.StructVector
    // https://jjbskir.medium.com/java-apache-arrow-read-and-write-to-a-listvector-df59809ef320
    // https://arrow.apache.org/docs/java/reference/index.html?org/apache/arrow/vector/complex/impl/UnionListWriter.html
    //https://github.com/apache/arrow/tree/master/java

    public static void main(String[] args) throws Exception
    {
        // the Assets Concept data for 3 Companies (Apple, Microsoft, IBM)
        List<String> ciks = Arrays.asList(  // "CIK0000320193", Apple
                                            "CIK0000789019", //Microsoft
                                          "CIK0000051143"); // IBM

        try(CompanyConcepts companyConcepts = new CompanyConcepts(ciks)){
            companyConcepts.print();
            companyConcepts.print();
        }
    }

    String[] UNIT_VARCHAR_FIELDS = new String[]{"accn", "form", "fp", "fy",
                                                "type"};  // type value options = [ 'USD', 'shares ]

    String[] UNIT_DATE_FIELDS = new String[]{"end", "filed"};

    Field COMPANY_CONCEPT_UNIT_FIELD() {

        List<Field> unitFields =
                Stream.concat(
                        Stream.of(UNIT_VARCHAR_FIELDS).map(fieldName -> new Field(fieldName, FieldType.nullable(MinorType.VARCHAR.getType()), null)),
                        Stream.of(UNIT_DATE_FIELDS).map(fieldName -> new Field(fieldName, FieldType.nullable(MinorType.DATEDAY.getType()), null))
                ).collect(Collectors.toList());

        unitFields.add(new Field("val", FieldType.nullable(MinorType.BIGINT.getType()), null));

        return new Field("unit", FieldType.nullable(MinorType.STRUCT.getType()), unitFields);
    }

    Field COMPANY_CONCEPT_UNITS_FIELD = new Field("units", FieldType.nullable(MinorType.LIST.getType()), Arrays.asList(COMPANY_CONCEPT_UNIT_FIELD()));

    Schema COMPANY_CONCEPT_SCHEMA()
    {
        List<Field> companyConceptFields = Stream.of(new String[]{"cik", "taxonomy", "tag", "label", "description", "entityName"})
                                                 .map(n -> new Field(n, FieldType.nullable(MinorType.VARCHAR.getType()), null))
                                                 .collect(Collectors.toList());

        companyConceptFields.add(COMPANY_CONCEPT_UNITS_FIELD);

        return new Schema(companyConceptFields, Map.of());
    }

    /*
        this would be cool, but can't get Dremio dependency working
        final FieldType mapType = CompleteType.struct(
                CompleteType.VARCHAR.toField("varchar"),
                CompleteType.INT.toField("int"),
                CompleteType.BIT.asList().toField("bits")
        ).toField("map").getFieldType();*/

    // new File("F:\\git-gms\\d-sci4j\\src\\main\\java\\org\\forj\\dsci\\arrow\\company_concept.json")
    public static Document getCompanyConcept(File file, String cik) throws IOException, InterruptedException, URISyntaxException
    {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(file);

        Document doc = Document.parse(node.toString());
        ArrayList<Document> units = (ArrayList)((Document)doc.remove("units")).get("USD");

        units.forEach(unit -> unit.append("type", "USD"));

        doc.append("units", units);
        doc.replace("cik", cik);

        return doc;
    }

    public static Document getCompanyConcept(String cik) throws IOException, InterruptedException, URISyntaxException
    {
        String url = "https://data.sec.gov/api/xbrl/companyconcept/" + cik + "/us-gaap/Assets.json";
        HttpClient httpClient = HttpClient.newBuilder().build();
        HttpRequest httpRequest = HttpRequest.newBuilder().GET()
                                             .uri(new URI(url))
                                             .headers("User-Agent", "gmsharpe@gmail.com")
                                             .build();

        HttpResponse response = httpClient.send(httpRequest,
                                                HttpResponse.BodyHandlers.ofString());

        Document doc = Document.parse(response.body().toString());

        ArrayList<Document> units = (ArrayList)((Document)doc.remove("units")).get("USD");

        units.forEach(unit -> unit.append("type", "USD"));

        doc.append("units", units);
        doc.replace("cik", cik);

        return doc;
    }

    @Override
    public void close()
    {
        root.close();
        allocator.close();
    }

    public void getConcept(List<Document> docs)
    {
        int row = 0;
        for (Document doc : docs) {
            for (Field nextField : COMPANY_CONCEPT_SCHEMA().getFields()) {
                Object value = TypeUtils.coerce(nextField, doc.get(nextField.getName()));
                Types.MinorType fieldType = Types.getMinorTypeForArrowType(nextField.getType());
                try {
                    // choices are the list of 'units' or a top level 'value' for the company concept
                    if (fieldType == MinorType.LIST) {
                        writeConceptUnitsList(root, allocator, nextField, value, row);
                    } else {
                        writeConceptFieldValue(root, allocator, nextField, value, row);
                    }
                }
                catch (Exception ex) {
                    throw new RuntimeException("Error while processing field " + nextField.getName(), ex);
                }
            }
            row++;
        }
        root.setRowCount(docs.size());
    }

    public ListVector units() {
        return (ListVector) root.getVector("units");
    }


    public void print() {
        ListVector listVector = (ListVector) root.getVector("units");
        // gets all units for all companies into a single struct vector
        StructVector unit = (StructVector)listVector.getChildrenFromFields().get(0);
        VectorSchemaRoot units_root = VectorSchemaRoot.of(unit.getChildrenFromFields().toArray(new FieldVector[]{}));

        // Company Concept
        System.out.println(root.contentToTSVString());

        // prints the json for the first company's units
        // listVector.getObject(0) gets the first item in the listVector
        JsonStringArrayList firstCompanyUnits = (JsonStringArrayList)listVector.getObject(0);
        //System.out.println(firstCompanyUnits.toString());


        // All Companies' Units
        System.out.println(units_root.contentToTSVString());
        System.out.println(units_root.getRowCount());
    }

    public void getConceptFor(String cik) {

        // todo - search the root level concept schema for index of cik?

        ListVector vector = units();

        TransferPair tp = vector.getTransferPair(allocator);
        tp.splitAndTransfer(0, 1);
        try (ListVector sliced = (ListVector) tp.getTo()) {
            System.out.println(sliced);
        }

        tp = vector.getTransferPair(allocator);
        // copy 6 elements from index 2
        tp.splitAndTransfer(1, 1);
        try (ListVector sliced = (ListVector) tp.getTo()) {
            System.out.print(sliced);
        }
        System.out.println(vector.size());
    }

}


