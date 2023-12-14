package org.sentrysoftware.tablejoin;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class TableJoinClientTests {

	@Test
	void exception() {
		assertAll("IllegalArgumentException",
				() -> assertThrows(IllegalArgumentException.class, () -> TableJoinClient.join("", "", 0, 1, ";", null, false, true), "leftKeyColumn < 1 is illegal"),
				() -> assertThrows(IllegalArgumentException.class, () -> TableJoinClient.join("", "", 1, 0, ";", null, false, true), "rightKeyColumn < 1 is illegal"),
				() -> assertThrows(IllegalArgumentException.class, () -> TableJoinClient.join("", "", 1, 1, "", null, false, true), "empty separator is illegal"),
				() -> assertThrows(IllegalArgumentException.class, () -> TableJoinClient.join("", "", 1, 1, null, null, false, true), "null separator is illegal")
		);
	}

	@Test
	void nullTable() {
		assertTrue(TableJoinClient.join(null, "", 1, 1, ";", null, false, true) == null);
	}

	@Test
	void emptyTable() {
		assertAll("Empty Table",
				() -> assertEquals("", TableJoinClient.join("", "", 1, 1, ";", null, false, true), "Empty left table must give empty result"),
				() -> assertEquals("", TableJoinClient.join("1;A", null, 1, 1, ";", null, false, true), "null right table must give empty result"),
				() -> assertEquals("", TableJoinClient.join("1;A", "", 1, 1, ";", null, false, true), "empty right table with null default right line must give empty result"),
				() -> assertEquals("", TableJoinClient.join("1;A", null, 1, 1, ";", "", false, true), "null right table with empty default right line must give empty result"),
				() -> assertEquals("", TableJoinClient.join("1;A", "", 1, 1, ";", "", false, true), "empty right table with empty default right line must give empty result")

		);
	}

	@Test
	void keyColumns() {
		String leftTable = "1;A;i;\n2;B;ii;\n3;C;iii;";
		String rightTable = "1;A;i;Good;\n3;C;iii;Good;";
		String expectedResult = "1;A;i;1;A;i;Good;\n3;C;iii;3;C;iii;Good;";
		assertAll("Key Columns",
				() -> assertEquals(expectedResult, TableJoinClient.join(leftTable, rightTable, 1, 1, ";", null, false, true), "leftKeyColumn = 1, rightKeyColumn = 1, Should match"),
				() -> assertEquals(expectedResult, TableJoinClient.join(leftTable, rightTable, 2, 2, ";", null, false, true), "leftKeyColumn = 1, rightKeyColumn = 1, Should match"),
				() -> assertEquals(expectedResult, TableJoinClient.join(leftTable, rightTable, 3, 3, ";", null, false, true), "leftKeyColumn = 1, rightKeyColumn = 1, Should match"),
				() -> assertEquals(expectedResult, TableJoinClient.join(leftTable, rightTable, 3, 3, ";", null, false, true), "leftKeyColumn = 1, rightKeyColumn = 1, Should match"),
				() -> assertEquals("", TableJoinClient.join(leftTable, rightTable, 1, 2, ";", null, false, true), "leftKeyColumn = 1, rightKeyColumn = 1, Should not match"),
				() -> assertEquals("", TableJoinClient.join(leftTable, rightTable, 1, 3, ";", null, false, true), "leftKeyColumn = 1, rightKeyColumn = 3, Should not match"),
				() -> assertEquals("", TableJoinClient.join(leftTable, rightTable, 2, 1, ";", null, false, true), "leftKeyColumn = 2, rightKeyColumn = 1, Should not match"),
				() -> assertEquals("", TableJoinClient.join(leftTable, rightTable, 2, 3, ";", null, false, true), "leftKeyColumn = 2, rightKeyColumn = 3, Should not match"),
				() -> assertEquals("", TableJoinClient.join(leftTable, rightTable, 3, 1, ";", null, false, true), "leftKeyColumn = 3, rightKeyColumn = 1, Should not match"),
				() -> assertEquals("", TableJoinClient.join(leftTable, rightTable, 3, 2, ";", null, false, true), "leftKeyColumn = 3, rightKeyColumn = 2, Should not match"),
				() -> assertEquals("", TableJoinClient.join(leftTable, rightTable, 100, 1, ";", null, false, true), "leftKeyColumn = 100, rightKeyColumn = 1, Should not match"),
				() -> assertEquals("", TableJoinClient.join(leftTable, rightTable, 1, 100, ";", null, false, true), "leftKeyColumn = 1, rightKeyColumn = 100, Should not match")
		);
	}

	@Test
	void caseInsensitive() {
		String leftTable = "1;A;i;\n2;B;ii;\n3;C;iii;";
		String rightTable = "1;a;I;Good;\n3;c;III;Good;";
		String expectedResult = "1;A;i;1;a;I;Good;\n3;C;iii;3;c;III;Good;";
		assertAll("Case Insensitive",
				() -> assertEquals(expectedResult, TableJoinClient.join(leftTable, rightTable, 2, 2, ";", null, false, true), "Should match despite case difference"),
				() -> assertEquals(expectedResult, TableJoinClient.join(leftTable, rightTable, 3, 3, ";", null, false, true), "Should match despite case difference"),
				() -> assertEquals("", TableJoinClient.join(leftTable, rightTable, 2, 2, ";", null, false, false), "Should not match because of case difference"),
				() -> assertEquals("", TableJoinClient.join(leftTable, rightTable, 3, 3, ";", null, false, false), "Should not match because of case difference")
		);
	}

	@Test
	void defaultRightLine() {
		String leftTable = "1;A;i;\n2;B;ii;\n3;C;iii;";
		String rightTable = "1;a;I;Good;\n3;c;III;Good;";
		String expectedResult = "1;A;i;1;a;I;Good;\n2;B;ii;default;right;line;\n3;C;iii;3;c;III;Good;";
		assertEquals(expectedResult, TableJoinClient.join(leftTable, rightTable, 1, 1, ";", "default;right;line;", false, true), "Should contain 3 lines because of default right line");
	}

	@Test
	void wbemKeyType() {
		String leftTable = "class.prop1=\"1\",prop2=\"A\";i;\nclass.prop1=\"2\",prop2=\"B\";ii;\nclass.prop1=\"3\",prop2=\"C\";iii;";
		String rightTable = "class.prop2=\"A\",prop1=\"1\";i;\nclass.prop2=\"C\",prop1=\"3\";iii;";
		String expectedResult = "class.prop1=\"1\",prop2=\"A\";i;class.prop2=\"A\",prop1=\"1\";i;\nclass.prop1=\"3\",prop2=\"C\";iii;class.prop2=\"C\",prop1=\"3\";iii;";
		assertAll("WBEM Key Type",
				() -> assertEquals(expectedResult, TableJoinClient.join(leftTable, rightTable, 1, 1, ";", null, true, true), "Should match despite different property order"),
				() -> assertEquals("", TableJoinClient.join(leftTable, rightTable, 1, 1, ";", null, false, true), "Should not match because of different property order")
		);
	}

	@Test
	void emptyLines() {
		String leftTable = "\n1;A;i;\n\n2;B;ii;\n\n\n3;C;iii;\n\n";
		String rightTable = "\n1;A;i;Good;\n\n3;C;iii;Good;\n\n";
		String expectedResult = "1;A;i;1;A;i;Good;\n3;C;iii;3;C;iii;Good;";
		assertEquals(expectedResult, TableJoinClient.join(leftTable, rightTable, 1, 1, ";", null, false, true), "Empty lines should be discarded from both tables");
	}

	@Test
	void testStringToTable() {
		final String csvTable = "\n1;A;i;\n\n2;B;ii;\n\n\n3;C;iii;\n\n";
		assertEquals(
				Stream.of(Arrays.asList("1", "A", "i"), Arrays.asList("2", "B", "ii"), Arrays.asList("3", "C", "iii"))
						.collect(Collectors.toList()),
				TableJoinClient.stringToTable(csvTable, ";"));

		assertNull(TableJoinClient.stringToTable(null, ";"));
		assertEquals(Collections.emptyList(), TableJoinClient.stringToTable("\n\n\n", ";"));
		final List<String> emptyCells = Arrays.asList("", "");
		assertEquals(Stream.of(emptyCells, emptyCells, emptyCells).collect(Collectors.toList()), TableJoinClient.stringToTable("\n;;\n;;\n;;", ";"));
		assertEquals(Stream.of(Arrays.asList("", "a"),emptyCells, emptyCells).collect(Collectors.toList()),
				TableJoinClient.stringToTable("\n;a;\n;;\n;;", ";"));
	}

	@Test
	void testLineToList() {
		assertEquals(Collections.emptyList(), TableJoinClient.lineToList(null, ";"));
		assertEquals(Collections.emptyList(), TableJoinClient.lineToList("", ";"));
		assertEquals(Arrays.asList(""), TableJoinClient.lineToList(";", ";"));
		assertEquals(Arrays.asList("", "", "", "", "", ""), TableJoinClient.lineToList(";;;;;;", ";"));
		assertEquals(Arrays.asList("","","a", "", "", ""), TableJoinClient.lineToList(";;a;;;;", ";"));
		assertEquals(Collections.emptyList(), TableJoinClient.lineToList("", ";"));

		// Test for line that ends without the ";" separator
		final List<String> list = Arrays.asList("a","b","c");
		assertEquals(list, TableJoinClient.lineToList("a;b;c", ";"));
		assertEquals(list, TableJoinClient.lineToList("a;b;c;", ";"));
	}

	@Test
	void testTableToString() {
		assertEquals(null, TableJoinClient.tableToString(null, ";"));
		assertEquals("", TableJoinClient.tableToString(Collections.emptyList(), ";"));
		assertEquals(";;;;;;",
				TableJoinClient.tableToString(Collections.singletonList(Arrays.asList("", "", "", "", "", "")), ";"));
		assertEquals(";;;;;;\n;;;;;;",
				TableJoinClient.tableToString(
						Stream.of(
										Arrays.asList("", "", "", "", "", ""),
										Arrays.asList("", "", "", "", "", ""))
								.collect(Collectors.toList()),
						";"));
		assertEquals(";;a;;;;\n;;;a;;;",
				TableJoinClient.tableToString(
						Stream.of(
										Arrays.asList("", "", "a", "", "", ""),
										Arrays.asList("", "", "", "a", "", ""))
								.collect(Collectors.toList()),
						";"));
	}
}
