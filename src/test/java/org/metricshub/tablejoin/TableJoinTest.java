package org.metricshub.tablejoin;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class TableJoinTest {

	@Test
	void exception() {
		assertAll(
			"IllegalArgumentException",
			() ->
				assertThrows(
					IllegalArgumentException.class,
					() -> TableJoin.join("", "", 0, 1, ";", null, false, true),
					"leftKeyColumn < 1 is illegal"
				),
			() ->
				assertThrows(
					IllegalArgumentException.class,
					() -> TableJoin.join("", "", 1, 0, ";", null, false, true),
					"rightKeyColumn < 1 is illegal"
				),
			() ->
				assertThrows(
					IllegalArgumentException.class,
					() -> TableJoin.join("", "", 1, 1, "", null, false, true),
					"empty separator is illegal"
				),
			() ->
				assertThrows(
					IllegalArgumentException.class,
					() -> TableJoin.join("", "", 1, 1, null, null, false, true),
					"null separator is illegal"
				)
		);
	}

	@Test
	void nullTable() {
		assertNull(TableJoin.join(null, "", 1, 1, ";", null, false, true));
	}

	@Test
	void emptyTable() {
		assertAll(
			"Empty Table",
			() ->
				assertEquals(
					"",
					TableJoin.join("", "", 1, 1, ";", null, false, true),
					"Empty left table must give empty result"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join("1;A", null, 1, 1, ";", null, false, true),
					"null right table must give empty result"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join("1;A", "", 1, 1, ";", null, false, true),
					"empty right table with null default right line must give empty result"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join("1;A", null, 1, 1, ";", "", false, true),
					"null right table with empty default right line must give empty result"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join("1;A", "", 1, 1, ";", "", false, true),
					"empty right table with empty default right line must give empty result"
				)
		);
	}

	@Test
	void keyColumns() {
		String leftTable = "1;A;i;\n2;B;ii;\n3;C;iii;";
		String rightTable = "1;A;i;Good;\n3;C;iii;Good;";
		String expectedResult = "1;A;i;1;A;i;Good;\n3;C;iii;3;C;iii;Good;";
		assertAll(
			"Key Columns",
			() ->
				assertEquals(
					expectedResult,
					TableJoin.join(leftTable, rightTable, 1, 1, ";", null, false, true),
					"leftKeyColumn = 1, rightKeyColumn = 1, Should match"
				),
			() ->
				assertEquals(
					expectedResult,
					TableJoin.join(leftTable, rightTable, 2, 2, ";", null, false, true),
					"leftKeyColumn = 1, rightKeyColumn = 1, Should match"
				),
			() ->
				assertEquals(
					expectedResult,
					TableJoin.join(leftTable, rightTable, 3, 3, ";", null, false, true),
					"leftKeyColumn = 1, rightKeyColumn = 1, Should match"
				),
			() ->
				assertEquals(
					expectedResult,
					TableJoin.join(leftTable, rightTable, 3, 3, ";", null, false, true),
					"leftKeyColumn = 1, rightKeyColumn = 1, Should match"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join(leftTable, rightTable, 1, 2, ";", null, false, true),
					"leftKeyColumn = 1, rightKeyColumn = 1, Should not match"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join(leftTable, rightTable, 1, 3, ";", null, false, true),
					"leftKeyColumn = 1, rightKeyColumn = 3, Should not match"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join(leftTable, rightTable, 2, 1, ";", null, false, true),
					"leftKeyColumn = 2, rightKeyColumn = 1, Should not match"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join(leftTable, rightTable, 2, 3, ";", null, false, true),
					"leftKeyColumn = 2, rightKeyColumn = 3, Should not match"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join(leftTable, rightTable, 3, 1, ";", null, false, true),
					"leftKeyColumn = 3, rightKeyColumn = 1, Should not match"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join(leftTable, rightTable, 3, 2, ";", null, false, true),
					"leftKeyColumn = 3, rightKeyColumn = 2, Should not match"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join(leftTable, rightTable, 100, 1, ";", null, false, true),
					"leftKeyColumn = 100, rightKeyColumn = 1, Should not match"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join(leftTable, rightTable, 1, 100, ";", null, false, true),
					"leftKeyColumn = 1, rightKeyColumn = 100, Should not match"
				)
		);
	}

	@Test
	void caseInsensitive() {
		String leftTable = "1;A;i;\n2;B;ii;\n3;C;iii;";
		String rightTable = "1;a;I;Good;\n3;c;III;Good;";
		String expectedResult = "1;A;i;1;a;I;Good;\n3;C;iii;3;c;III;Good;";
		assertAll(
			"Case Insensitive",
			() ->
				assertEquals(
					expectedResult,
					TableJoin.join(leftTable, rightTable, 2, 2, ";", null, false, true),
					"Should match despite case difference"
				),
			() ->
				assertEquals(
					expectedResult,
					TableJoin.join(leftTable, rightTable, 3, 3, ";", null, false, true),
					"Should match despite case difference"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join(leftTable, rightTable, 2, 2, ";", null, false, false),
					"Should not match because of case difference"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join(leftTable, rightTable, 3, 3, ";", null, false, false),
					"Should not match because of case difference"
				)
		);
	}

	@Test
	void defaultRightLine() {
		String leftTable = "1;A;i;\n2;B;ii;\n3;C;iii;";
		String rightTable = "1;a;I;Good;\n3;c;III;Good;";
		String expectedResult = "1;A;i;1;a;I;Good;\n2;B;ii;default;right;line;\n3;C;iii;3;c;III;Good;";
		assertEquals(
			expectedResult,
			TableJoin.join(leftTable, rightTable, 1, 1, ";", "default;right;line;", false, true),
			"Should contain 3 lines because of default right line"
		);
	}

	@Test
	void wbemKeyType() {
		String leftTable =
			"class.prop1=\"1\",prop2=\"A\";i;\nclass.prop1=\"2\",prop2=\"B\";ii;\nclass.prop1=\"3\",prop2=\"C\";iii;";
		String rightTable = "class.prop2=\"A\",prop1=\"1\";i;\nclass.prop2=\"C\",prop1=\"3\";iii;";
		String expectedResult =
			"class.prop1=\"1\",prop2=\"A\";i;class.prop2=\"A\",prop1=\"1\";i;\nclass.prop1=\"3\",prop2=\"C\";iii;class.prop2=\"C\",prop1=\"3\";iii;";
		assertAll(
			"WBEM Key Type",
			() ->
				assertEquals(
					expectedResult,
					TableJoin.join(leftTable, rightTable, 1, 1, ";", null, true, true),
					"Should match despite different property order"
				),
			() ->
				assertEquals(
					"",
					TableJoin.join(leftTable, rightTable, 1, 1, ";", null, false, true),
					"Should not match because of different property order"
				)
		);
	}

	@Test
	void emptyLines() {
		String leftTable = "\n1;A;i;\n\n2;B;ii;\n\n\n3;C;iii;\n\n";
		String rightTable = "\n1;A;i;Good;\n\n3;C;iii;Good;\n\n";
		String expectedResult = "1;A;i;1;A;i;Good;\n3;C;iii;3;C;iii;Good;";
		assertEquals(
			expectedResult,
			TableJoin.join(leftTable, rightTable, 1, 1, ";", null, false, true),
			"Empty lines should be discarded from both tables"
		);
	}

	@Test
	void testStringToTable() {
		final String csvTable = "\n1;A;i;\n\n2;B;ii;\n\n\n3;C;iii;\n\n";
		assertEquals(
			Stream
				.of(Arrays.asList("1", "A", "i"), Arrays.asList("2", "B", "ii"), Arrays.asList("3", "C", "iii"))
				.collect(Collectors.toList()),
			TableJoin.stringToTable(csvTable, ";")
		);

		assertNull(TableJoin.stringToTable(null, ";"));
		assertEquals(Collections.emptyList(), TableJoin.stringToTable("\n\n\n", ";"));
		final List<String> emptyCells = Arrays.asList("", "");
		assertEquals(
			Stream.of(emptyCells, emptyCells, emptyCells).collect(Collectors.toList()),
			TableJoin.stringToTable("\n;;\n;;\n;;", ";")
		);
		assertEquals(
			Stream.of(Arrays.asList("", "a"), emptyCells, emptyCells).collect(Collectors.toList()),
			TableJoin.stringToTable("\n;a;\n;;\n;;", ";")
		);
	}

	@Test
	void testLineToList() {
		assertEquals(Collections.emptyList(), TableJoin.lineToList(null, ";"));
		assertEquals(Collections.emptyList(), TableJoin.lineToList("", ";"));
		assertEquals(Arrays.asList(""), TableJoin.lineToList(";", ";"));
		assertEquals(Arrays.asList("", "", "", "", "", ""), TableJoin.lineToList(";;;;;;", ";"));
		assertEquals(Arrays.asList("", "", "a", "", "", ""), TableJoin.lineToList(";;a;;;;", ";"));
		assertEquals(Collections.emptyList(), TableJoin.lineToList("", ";"));

		// Test for line that ends without the ";" separator
		final List<String> list = Arrays.asList("a", "b", "c");
		assertEquals(list, TableJoin.lineToList("a;b;c", ";"));
		assertEquals(list, TableJoin.lineToList("a;b;c;", ";"));
	}

	@Test
	void testTableToString() {
		assertEquals(null, TableJoin.tableToString(null, ";"));
		assertEquals("", TableJoin.tableToString(Collections.emptyList(), ";"));
		assertEquals(
			";;;;;;",
			TableJoin.tableToString(Collections.singletonList(Arrays.asList("", "", "", "", "", "")), ";")
		);
		assertEquals(
			";;;;;;\n;;;;;;",
			TableJoin.tableToString(
				Stream
					.of(Arrays.asList("", "", "", "", "", ""), Arrays.asList("", "", "", "", "", ""))
					.collect(Collectors.toList()),
				";"
			)
		);
		assertEquals(
			";;a;;;;\n;;;a;;;",
			TableJoin.tableToString(
				Stream
					.of(Arrays.asList("", "", "a", "", "", ""), Arrays.asList("", "", "", "a", "", ""))
					.collect(Collectors.toList()),
				";"
			)
		);
	}
}
