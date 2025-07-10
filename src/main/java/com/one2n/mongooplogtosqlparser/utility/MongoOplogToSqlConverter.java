package com.one2n.mongooplogtosqlparser.utility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Utility class that converts MongoDB oplog entries to equivalent SQL statements.
 */
public class MongoOplogToSqlConverter {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Converts a MongoDB oplog JSON entry into an equivalent SQL insert statement asynchronously.
     * @param oplogJson The oplog entry as a JSON string.
     * @return A CompletableFuture that will complete with the corresponding SQL statement.
     */
    public CompletableFuture<String> convertOplogToSqlAsync(String oplogJson) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                JsonNode root = objectMapper.readTree(oplogJson);
                String op = root.get("op").asText();
                if (!"i".equals(op)) {
                    throw new IllegalArgumentException("Only insert operations (op: 'i') are supported");
                }

                // Extract db and collection
                String ns = root.get("ns").asText();
                String[] nsParts = ns.split("\\.", 2);
                if (nsParts.length != 2) {
                    throw new IllegalArgumentException("Invalid ns format: " + ns);
                }
                String database = nsParts[0];
                String collection = nsParts[1];

                // Extract fields and values
                JsonNode document = root.get("o");
                String columns = StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(document.fields(), Spliterator.ORDERED),
                                false
                        ).map(Map.Entry::getKey)
                        .collect(Collectors.joining(", "));

                String values = StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(document.fields(), Spliterator.ORDERED),
                                false)
                        .map(entry -> {
                            JsonNode value = entry.getValue();
                            if (value.isTextual()) {
                                return "'" + value.asText().replace("'", "''") + "'"; // Escape single quotes
                            } else if (value.isNumber()) {
                                return value.asText();
                            } else if (value.isBoolean()) {
                                return String.valueOf(value.asBoolean());
                            } else {
                                throw new IllegalArgumentException("Unsupported field type: " + value.getNodeType());
                            }
                        })
                        .collect(Collectors.joining(", "));

                return String.format("INSERT INTO %s.%s (%s) VALUES (%s);", database, collection, columns, values);
            } catch (Exception e) {
                throw new RuntimeException("Failed to convert oplog to SQL: " + e.getMessage(), e);
            }
        });
    }
}
