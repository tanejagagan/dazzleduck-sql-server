package io.dazzleduck.sql.commons.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HeaderUtilsTest {

    // ==================== parseCsv tests ====================

    @Test
    void parseCsv_nullValue_returnsEmptyArray() {
        assertArrayEquals(new String[0], HeaderUtils.parseCsv(null));
    }

    @Test
    void parseCsv_emptyValue_returnsEmptyArray() {
        assertArrayEquals(new String[0], HeaderUtils.parseCsv(""));
        assertArrayEquals(new String[0], HeaderUtils.parseCsv("   "));
    }

    @Test
    void parseCsv_simpleValues_returnsParsedArray() {
        assertArrayEquals(new String[]{"a", "b", "c"}, HeaderUtils.parseCsv("a,b,c"));
    }

    @Test
    void parseCsv_quotedValues_handlesQuotesCorrectly() {
        assertArrayEquals(new String[]{"a,b", "c"}, HeaderUtils.parseCsv("\"a,b\",c"));
    }

    @Test
    void parseCsv_trimWhitespace_trimsValues() {
        assertArrayEquals(new String[]{"a", "b", "c"}, HeaderUtils.parseCsv(" a , b , c "));
    }

    @Test
    void parseCsv_emptyFields_skipsEmptyValues() {
        assertArrayEquals(new String[]{"a", "c"}, HeaderUtils.parseCsv("a,,c"));
    }

    // ==================== parseColumnReferences tests ====================

    @Test
    void parseColumnReferences_validColumns_returnsQuotedArray() {
        // Input: column1,column2 -> Output: "column1","column2"
        assertArrayEquals(
                new String[]{"\"column1\"", "\"column2\""},
                HeaderUtils.parseColumnReferences("column1,column2")
        );
    }

    @Test
    void parseColumnReferences_singleColumn_returnsQuotedArray() {
        assertArrayEquals(
                new String[]{"\"my_column\""},
                HeaderUtils.parseColumnReferences("my_column")
        );
    }

    @Test
    void parseColumnReferences_underscorePrefix_allowed() {
        assertArrayEquals(
                new String[]{"\"_private\""},
                HeaderUtils.parseColumnReferences("_private")
        );
    }

    @Test
    void parseColumnReferences_specialChars_quotedSafely() {
        // Special characters are safely quoted
        assertArrayEquals(
                new String[]{"\"column-name\""},
                HeaderUtils.parseColumnReferences("column-name")
        );
    }

    @Test
    void parseColumnReferences_withQuotes_escapedProperly() {
        // Quotes in input are escaped by doubling
        assertArrayEquals(
                new String[]{"\"col\"\"name\""},
                HeaderUtils.parseColumnReferences("col\"name")
        );
    }

    @Test
    void parseColumnReferences_emptyValue_returnsEmptyArray() {
        assertArrayEquals(new String[0], HeaderUtils.parseColumnReferences(""));
        assertArrayEquals(new String[0], HeaderUtils.parseColumnReferences(null));
    }

    // ==================== parseSortOrder tests ====================

    @Test
    void parseSortOrder_columnWithAsc_returnsQuotedWithDirection() {
        assertArrayEquals(
                new String[]{"\"column1\" ASC"},
                HeaderUtils.parseSortOrder("column1 ASC")
        );
    }

    @Test
    void parseSortOrder_columnWithDesc_returnsQuotedWithDirection() {
        assertArrayEquals(
                new String[]{"\"column1\" DESC"},
                HeaderUtils.parseSortOrder("column1 DESC")
        );
    }

    @Test
    void parseSortOrder_columnWithLowercaseDirection_normalizesToUppercase() {
        assertArrayEquals(
                new String[]{"\"column1\" ASC"},
                HeaderUtils.parseSortOrder("column1 asc")
        );
        assertArrayEquals(
                new String[]{"\"column1\" DESC"},
                HeaderUtils.parseSortOrder("column1 desc")
        );
    }

    @Test
    void parseSortOrder_columnWithoutDirection_returnsQuotedOnly() {
        assertArrayEquals(
                new String[]{"\"column1\""},
                HeaderUtils.parseSortOrder("column1")
        );
    }

    @Test
    void parseSortOrder_multipleColumns_handlesEachCorrectly() {
        assertArrayEquals(
                new String[]{"\"col1\" ASC", "\"col2\" DESC", "\"col3\""},
                HeaderUtils.parseSortOrder("col1 ASC,col2 DESC,col3")
        );
    }

    @Test
    void parseSortOrder_columnWithUnderscores_handlesCorrectly() {
        assertArrayEquals(
                new String[]{"\"created_at\" DESC", "\"updated_at\" ASC"},
                HeaderUtils.parseSortOrder("created_at DESC,updated_at ASC")
        );
    }

    @Test
    void parseSortOrder_emptyValue_returnsEmptyArray() {
        assertArrayEquals(new String[0], HeaderUtils.parseSortOrder(""));
        assertArrayEquals(new String[0], HeaderUtils.parseSortOrder(null));
    }

    @Test
    void parseSortOrder_columnWithQuotes_escapedProperly() {
        assertArrayEquals(
                new String[]{"\"col\"\"name\" ASC"},
                HeaderUtils.parseSortOrder("col\"name ASC")
        );
    }

    // ==================== parseExpressions tests ====================

    @Test
    void parseExpressions_simpleExpression_returnsArray() {
        assertArrayEquals(
                new String[]{"col1 + col2", "col3"},
                HeaderUtils.parseExpressions("col1 + col2,col3")
        );
    }

    @Test
    void parseExpressions_expressionWithAs_allowed() {
        assertArrayEquals(
                new String[]{"col1 + col2 as total"},
                HeaderUtils.parseExpressions("col1 + col2 as total")
        );
    }

    @Test
    void parseExpressions_functionCall_allowed() {
        // For functions with commas, use CSV quoting
        assertArrayEquals(
                new String[]{"CONCAT(a, b)", "UPPER(name)"},
                HeaderUtils.parseExpressions("\"CONCAT(a, b)\",UPPER(name)")
        );
    }

    @Test
    void parseExpressions_selectKeyword_throwsException() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> HeaderUtils.parseExpressions("SELECT * FROM users")
        );
        assertTrue(ex.getMessage().contains("SELECT"));
    }

    @Test
    void parseExpressions_unionKeyword_throwsException() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> HeaderUtils.parseExpressions("col1 UNION SELECT")
        );
        assertTrue(ex.getMessage().contains("UNION"));
    }

    @Test
    void parseExpressions_dropKeyword_throwsException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> HeaderUtils.parseExpressions("col1; DROP TABLE users")
        );
    }

    @Test
    void parseExpressions_sqlComment_throwsException() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> HeaderUtils.parseExpressions("col1 -- comment")
        );
        assertTrue(ex.getMessage().contains("comment"));
    }

    @Test
    void parseExpressions_blockComment_throwsException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> HeaderUtils.parseExpressions("col1 /* comment */")
        );
    }

    @Test
    void parseExpressions_semicolon_throwsException() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> HeaderUtils.parseExpressions("col1; col2")
        );
        assertTrue(ex.getMessage().contains("Semicolon"));
    }

    @Test
    void parseExpressions_keywordWithinWord_allowed() {
        // "SELECTED" contains "SELECT" but is not the keyword surrounded by spaces
        assertArrayEquals(
                new String[]{"SELECTED_COLUMN"},
                HeaderUtils.parseExpressions("SELECTED_COLUMN")
        );
    }

    @Test
    void parseExpressions_keywordAtStart_throwsException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> HeaderUtils.parseExpressions("SELECT col1")
        );
    }

    @Test
    void parseExpressions_keywordAtEnd_throwsException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> HeaderUtils.parseExpressions("col1 FROM")
        );
    }

    @Test
    void parseExpressions_caseInsensitive_detectsLowercase() {
        assertThrows(
                IllegalArgumentException.class,
                () -> HeaderUtils.parseExpressions("select * from users")
        );
    }

    @Test
    void parseExpressions_emptyValue_returnsEmptyArray() {
        assertArrayEquals(new String[0], HeaderUtils.parseExpressions(""));
        assertArrayEquals(new String[0], HeaderUtils.parseExpressions(null));
    }

    // ==================== quoteIdentifier tests ====================

    @Test
    void quoteIdentifier_simpleValue_addsQuotes() {
        assertEquals("\"column\"", HeaderUtils.quoteIdentifier("column"));
    }

    @Test
    void quoteIdentifier_withQuotes_escapesQuotes() {
        assertEquals("\"col\"\"name\"", HeaderUtils.quoteIdentifier("col\"name"));
    }

    // ==================== validateExpression tests ====================

    @Test
    void validateExpression_validExpression_noException() {
        assertDoesNotThrow(() -> HeaderUtils.validateExpression("col1 + col2"));
        assertDoesNotThrow(() -> HeaderUtils.validateExpression("'literal string'"));
        assertDoesNotThrow(() -> HeaderUtils.validateExpression("CASE WHEN x > 1 THEN 'a' ELSE 'b' END"));
    }

    @Test
    void validateExpression_blank_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> HeaderUtils.validateExpression(""));
        assertThrows(IllegalArgumentException.class, () -> HeaderUtils.validateExpression("   "));
        assertThrows(IllegalArgumentException.class, () -> HeaderUtils.validateExpression(null));
    }
}
