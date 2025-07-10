package com.one2n.mongooplogtosqlparser.utility;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link MongoOplogToSqlConverter}.
 * <p>
 * This class verifies the correct conversion of MongoDB oplog entries
 * into SQL statements. It includes tests for various scenarios such as:
 * <ul>
 *   <li>Insert operations with complete and partial field sets</li>
 *   <li>Handling of blank or null field values</li>
 *   <li>Unsupported or malformed input</li>
 * </ul>
 */
class MongoOplogToSqlConverterTest {

    private MongoOplogToSqlConverter converter;

    @BeforeEach
    void setUp() {
        converter = new MongoOplogToSqlConverter();
    }

    /**
     * Tests that a valid MongoDB insert oplog entry is correctly converted
     * into an equivalent SQL INSERT statement.
     * @throws ExecutionException if the asynchronous operation fails
     * @throws InterruptedException if the thread executing the test is interrupted
     */
    @Test
    void testValidInsertOplog() throws ExecutionException, InterruptedException {
        String oplogJson = """
                {
                    "op": "i",
                    "ns": "test.student",
                    "o": {
                        "_id": "635b79e231d82a8ab1de863b",
                        "name": "Selena Miller",
                        "roll_no": 51,
                        "is_graduated": false,
                        "date_of_birth": "2000-01-30"
                    }
                }
                """;
        String expectedSql = "INSERT INTO test.student (_id, name, roll_no, is_graduated, date_of_birth) VALUES ('635b79e231d82a8ab1de863b', 'Selena Miller', 51, false, '2000-01-30');";

        String actualSql = converter.convertOplogToSqlAsync(oplogJson).get();
        assertEquals(expectedSql, actualSql, "Generated SQL INSERT statement should match expected format");
    }

    /**
     * Tests that a non-insert operations throw exception
     * @throws ExecutionException if the asynchronous operation fails
     * @throws InterruptedException if the thread executing the test is interrupted
     */
    @Test
    void testInvalidOpType() throws ExecutionException, InterruptedException {
        String oplogJson = """
                {
                    "op": "u",
                    "ns": "test.student",
                    "o": {
                        "_id": "635b79e231d82a8ab1de863b",
                        "name": "Selena Miller"
                    }
                }
                """;
        assertThrows(ExecutionException.class, () -> {
            converter.convertOplogToSqlAsync(oplogJson).get();
        }, "Should throw exception for non-insert operation");
    }

    /**
     * Tests invalid json throws exception
     * @throws ExecutionException if the asynchronous operation fails
     * @throws InterruptedException if the thread executing the test is interrupted
     */
    @Test
    void testInvalidNsFormat() throws ExecutionException, InterruptedException {
        String oplogJson = """
                {
                    "op": "i",
                    "ns": "test",
                    "o": {
                        "_id": "635b79e231d82a8ab1de863b",
                        "name": "Selena Miller"
                    }
                }
                """;
        assertThrows(ExecutionException.class, () -> {
            converter.convertOplogToSqlAsync(oplogJson).get();
        }, "Should throw exception for invalid ns format");
    }


    /**
     * Tests that a valid MongoDB insert oplog entry is correctly converted
     * into an equivalent SQL INSERT statement regardless the order.
     * @throws ExecutionException if the asynchronous operation fails
     * @throws InterruptedException if the thread executing the test is interrupted
     */
    @Test
    void testDifferentFieldOrder() throws ExecutionException, InterruptedException {
        String oplogJson = """
                {
                    "op": "i",
                    "ns": "test.student",
                    "o": {
                        "name": "John Doe",
                        "roll_no": 42,
                        "_id": "635b79e231d82a8ab1de863c",
                        "date_of_birth": "1999-12-15",
                        "is_graduated": true
                    }
                }
                """;
        String expectedSql = "INSERT INTO test.student (name, roll_no, _id, date_of_birth, is_graduated) VALUES ('John Doe', 42, '635b79e231d82a8ab1de863c', '1999-12-15', true);";

        String actualSql = converter.convertOplogToSqlAsync(oplogJson).get();
        assertEquals(expectedSql, actualSql, "Generated SQL should match expected INSERT statement based on oplog entry");
    }

    /**
     * Tests that single quote escape
     * @throws ExecutionException if the asynchronous operation fails
     * @throws InterruptedException if the thread executing the test is interrupted
     */
    @Test
    void testStringWithSingleQuotes() throws ExecutionException, InterruptedException {
        String oplogJson = """
                {
                    "op": "i",
                    "ns": "test.student",
                    "o": {
                        "_id": "635b79e231d82a8ab1de863d",
                        "name": "O'Neil",
                        "roll_no": 53,
                        "is_graduated": true,
                        "date_of_birth": "2001-02-14"
                    }
                }
                """;
        String expectedSql = "INSERT INTO test.student (_id, name, roll_no, is_graduated, date_of_birth) VALUES ('635b79e231d82a8ab1de863d', 'O''Neil', 53, true, '2001-02-14');";

        String actualSql = converter.convertOplogToSqlAsync(oplogJson).get();
        assertEquals(expectedSql, actualSql, "Generated SQL INSERT statement should escape single quotes");
    }

    /**
     * Tests that a valid MongoDB insert oplog entry is correctly converted
     * into an equivalent SQL INSERT statement with blank field values.
     * @throws ExecutionException if the asynchronous operation fails
     * @throws InterruptedException if the thread executing the test is interrupted
     */
    @Test
    void testBlankFieldValues() throws ExecutionException, InterruptedException {
        String oplogJson = """
                {
                    "op": "i",
                    "ns": "test.student",
                    "o": {
                        "_id": "635b79e231d82a8ab1de863d",
                        "name": "",
                        "roll_no": 53,
                        "is_graduated": true,
                        "date_of_birth": "2001-02-14"
                    }
                }
                """;
        String expectedSql = "INSERT INTO test.student (_id, name, roll_no, is_graduated, date_of_birth) VALUES ('635b79e231d82a8ab1de863d', '', 53, true, '2001-02-14');";

        String actualSql = converter.convertOplogToSqlAsync(oplogJson).get();
        assertEquals(expectedSql, actualSql, "Generated SQL INSERT should include fields with blank values without errors");
    }

    /**
     * Tests that a valid MongoDB insert oplog entry is correctly converted
     * into an equivalent SQL INSERT statement with less fields.
     * @throws ExecutionException if the asynchronous operation fails
     * @throws InterruptedException if the thread executing the test is interrupted
     */
    @Test
    void testLessFields() throws ExecutionException, InterruptedException {
        String oplogJson = """
                {
                    "op": "i",
                    "ns": "test.student",
                    "o": {
                        "_id": "635b79e231d82a8ab1de863d",
                        "name": "neil"
                    }
                }
                """;
        String expectedSql = "INSERT INTO test.student (_id, name) VALUES ('635b79e231d82a8ab1de863d', 'neil');";

        String actualSql = converter.convertOplogToSqlAsync(oplogJson).get();
        assertEquals(expectedSql, actualSql, "Generated SQL should match expected INSERT statement based on oplog entry");
    }

    /**
     * Tests that a valid MongoDB insert oplog entry is correctly converted
     * into an equivalent SQL INSERT statement.
     * @throws ExecutionException if the asynchronous operation fails
     * @throws InterruptedException if the thread executing the test is interrupted
     */
    @Test
    void testDifferentFields() throws ExecutionException, InterruptedException {
        String oplogJson = """
                {
                    "op": "i",
                    "ns": "test.student",
                    "o": {
                        "id": "635b79e231d82a8ab1de863d",
                        "name1": "neil",
                        "roll_no1": 53
                    }
                }
                """;
        String expectedSql = "INSERT INTO test.student (id, name1, roll_no1) VALUES ('635b79e231d82a8ab1de863d', 'neil', 53);";

        String actualSql = converter.convertOplogToSqlAsync(oplogJson).get();
        assertEquals(expectedSql, actualSql, "Generated SQL should match expected INSERT statement based on oplog entry");
    }

    /**
     * Nested Json not handled
     * @throws ExecutionException if the asynchronous operation fails
     * @throws InterruptedException if the thread executing the test is interrupted
     */
    @Test
    void testNestedJson() throws ExecutionException, InterruptedException {
        String oplogJson = """
                {
                    "op": "i",
                    "ns": "test.student",
                    "o": {
                        "id": "635b79e231d82a8ab1de863d",
                        "name1": {
                        "name" : "test"
                        },
                        "roll_no1": 53
                    }
                }
                """;
        assertThrows(ExecutionException.class, () -> {
            converter.convertOplogToSqlAsync(oplogJson).get();
        }, "Should throw exception for invalid ns format");
    }
}