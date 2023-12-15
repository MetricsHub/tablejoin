/*-
 * ╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲
 * TableJoin Utility
 * ჻჻჻჻჻჻
 * Copyright (C) 2023 Sentry Software
 * ჻჻჻჻჻჻
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱
 */
package org.sentrysoftware.tablejoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Joins to CSV-formatted tables (Strings) and as well as Lists formatted tables
 *
 * @author bertrand
 *
 */
public class TableJoinClient {

	private TableJoinClient() {

	}

	/**
	 * Joins to CSV-formatted tables (Strings) as an SQL JOIN statement would (INNER or LEFT JOIN)
	 * <p>
	 *
	 * @param leftTable
	 *             The left table (entries are separated by end-of-lines \n)
	 * @param rightTable
	 *             The right table
	 * @param leftKeyColumnNumber
	 *             The number of the column that will be used as key in the left table
	 * @param rightKeyColumnNumber
	 *             The number of the column that will be used as key in the right table
	 * @param separator
	 *             The columns separator
	 * @param defaultRightLine
	 *             Specify a default entry to be put if a matching entry is not found in the right table (LEFT JOIN). Leave empty or null if you want an INNER JOIN.
	 * @param wbemKeyType
	 *             Whether the key is in the form of a WBEM path, which needs to be sorted before searching for matches
	 * @param caseInsensitive
	 *             Whether the matching is done case insensitive
	 * @return The result of the JOIN operation, formatted as a CSV table (one entry per line, with the same separator as specified)
	 * @throws IllegalArgumentException
	 *             When one of the arguments prevents the operation from working safely
	 */
	public static String join(
			final String leftTable,
			final String rightTable,
			final int leftKeyColumnNumber,
			final int rightKeyColumnNumber,
			final String separator,
			final String defaultRightLine,
			final boolean wbemKeyType,
			final boolean caseInsensitive) throws IllegalArgumentException {

		if (separator == null || "".equals(separator)) {
			throw new IllegalArgumentException("Separator cannot be null or empty");
		}

		final List<List<String>> leftTableList = stringToTable(leftTable, separator);
		final List<List<String>> rightTableList = stringToTable(rightTable, separator);
		// The default line separator is always semicolon
		final List<String> defaultRightLineList = lineToList(defaultRightLine, ";");

		final List<List<String>> result = join(
				leftTableList,
				rightTableList,
				leftKeyColumnNumber,
				rightKeyColumnNumber,
				defaultRightLineList,
				wbemKeyType,
				caseInsensitive);

		return tableToString(result, separator);
	}

	/**
	 * Transform the {@link List} table to a {@link String} representation
	 * [[a1,b1,c2],[a1,b1,c1]]
	 *  =&gt;
	 * a1,b1,c1,
	 * a2,b2,c2,
	 *
	 * @param table The table result we wish to parse
	 * @param separator The cells separator on each line
	 * @return {@link String} value
	 */
	protected static String tableToString(final List<List<String>> table, final String separator) {
		if (table != null) {
			return table
					.stream()
					.filter(Objects::nonNull)
					.map(line -> line
							.stream()
							.collect(Collectors.joining(separator)) + separator)
					.collect(Collectors.joining("\n"));
		}
		return null;
	}

	/**
	 * Return the List representation of the CSV String table :
	 * a1,b1,c1,
	 * a2,b2,c2,
	 * =&gt;
	 * [[a1,b1,c2],[a1,b1,c1]]
	 *
	 * @param csvTable
	 *             The CSV table we wish to parse
	 * @param separator
	 *             The cells separator
	 * @return {@link List} of {@link List} table
	 */
	protected static List<List<String>> stringToTable(final String csvTable, final String separator) {
		if (csvTable != null) {
			return Stream
					.of(csvTable.split("\n"))
					.map(line -> lineToList(line, separator))
					.filter(line -> !line.isEmpty())
					.collect(Collectors.toList());
		}
		return null;
	}

	/**
	 * Transform a line to a list
	 * a1,b1,c1, =&gt; [ a1, b1, c1 ]
	 * @param line
	 *             The CSV line we wish to parse
	 * @param separator
	 *             The cells serparator
	 * @return {@link List} of {@link String}
	 */
	protected static List<String> lineToList(String line, final String separator) {
		if (line != null && !line.isEmpty()) {

			// Make sure the line ends with the separator
			line = !line.endsWith(separator) ? line + separator : line;

			// Make sure we don't change the integrity of the line with the split in case of empty cells
			final String[] split = line.split(separator, -1);
			return Stream
					.of(split)
					.limit(split.length - 1L)
					.collect(Collectors.toList());
		}
		return new ArrayList<>();
	}

	/**
	 * Joins two tables ({@link List} of {@link List}) as an SQL JOIN statement
	 * would (INNER or LEFT JOIN)
	 * <p>
	 *
	 * @param leftTable
	 *             The left table (entries are defined in a {@link List} of {@link String} values)
	 * @param rightTable
	 *             The right table
	 * @param leftKeyColumnNumber
	 *             The number of the column that will be used as key in the left table
	 * @param rightKeyColumnNumber
	 *             The number of the column that will be used as key in the right table
	 * @param defaultRightLine
	 *             Specify a default entry to be put if a matching  entry is not found in the right table (LEFT JOIN).
	 *             Leave empty or null if you want an INNER JOIN.
	 * @param wbemKeyType
	 *             Whether the key is in the form of a WBEM path, which needs to be sorted before searching for matches
	 * @param caseInsensitive
	 *             Whether the matching is done case insensitive
	 * @return The result of the JOIN operation, a {@link List} of {@link List} table (one entry per {@link List} wrapped in a parent {@link List})
	 * @throws IllegalArgumentException
	 *             When one of the arguments prevents the operation from working safely
	 */
	public static List<List<String>> join(final List<List<String>> leftTable, final List<List<String>> rightTable,
										  final int leftKeyColumnNumber, final int rightKeyColumnNumber, final List<String> defaultRightLine,
										  final boolean wbemKeyType, final boolean caseInsensitive) throws IllegalArgumentException {

		// Sanity check
		if (leftKeyColumnNumber < 1 || rightKeyColumnNumber < 1) {
			throw new IllegalArgumentException("Invalid key column number (leftKeyColumnNumber=" + leftKeyColumnNumber
					+ ", rightKeyColumnNumber=" + rightKeyColumnNumber + ")");
		}

		if (leftTable == null) {
			return null;
		}

		// LEFT JOIN
		final boolean handleDefaultRightLine = defaultRightLine != null && !defaultRightLine.isEmpty();
		if (rightTable == null && !handleDefaultRightLine) {
			return new ArrayList<>();
		}

		final Map<String, List<String>> rightTableLookup;

		// Initialize the lookup table (a hash map)
		if (null != rightTable) {

			rightTableLookup = rightTable
					.stream()
					.filter(line -> rightKeyColumnNumber <= line.size())
					.collect(Collectors.toMap(
							line -> getKey(line, rightKeyColumnNumber, wbemKeyType, caseInsensitive),
							Function.identity(),
							(oldValue, newValue) -> oldValue));
		} else {
			rightTableLookup = new HashMap<>();
		}

		// Stream the left table, line by line
		return leftTable
				.stream()
				.filter(leftLine -> isValidLeftLine(leftKeyColumnNumber, leftLine))
				.map(leftLine -> joinLine(
						leftLine,
						leftKeyColumnNumber,
						defaultRightLine,
						wbemKeyType,
						caseInsensitive,
						handleDefaultRightLine,
						rightTableLookup))
				.filter(line -> !line.isEmpty())
				.collect(Collectors.toList());
	}

	/**
	 * Extract the key value from the given table line (row)
	 * @param line
	 *             The line we wish to process to extract the key
	 * @param keyColumnNumber
	 *             The index of the key column
	 * @param wbemKeyType
	 *             Whether the key is in the form of a WBEM path, which needs to be sorted before searching for matches
	 * @param caseInsensitive
	 *             Whether the matching is done case insensitive
	 * @return {@link String} value
	 */
	protected static String getKey(final List<String> line, final int keyColumnNumber, final boolean wbemKeyType,
								   final boolean caseInsensitive) {
		// Extract the key column from the line
		String key = line.get(keyColumnNumber - 1);
		// Special key options
		if (caseInsensitive) {
			key = key.toLowerCase();
		}
		if (wbemKeyType) {
			key = normalizeWbemReference(key);
		}
		return key;
	}

	/**
	 * Check if the given left line is valid
	 * <ol>
	 * 		<li>The leftKeyColumnNumber must be less than or equal to the leftLine size</li>
	 * 		<li>The leftLine shouldn't be empty</li>
	 * 		<li>The leftLine key at leftKeyColumnNumber shouldn't be empty</li>
	 * </ol>
	 * @param leftKeyColumnNumber
	 *             The number of the column that will be used as key in the left line
	 * @param leftLine
	 *             The left line as {@link List}
	 * @return <code>true</code> if the left line matches the conditions above
	 */
	protected static boolean isValidLeftLine(final int leftKeyColumnNumber, final List<String> leftLine) {
		return leftKeyColumnNumber <= leftLine.size()
				&& !leftLine.isEmpty()
				&& !leftLine.get(leftKeyColumnNumber - 1).isEmpty();
	}

	/**
	 * Execute the INNER or LEFT JOIN on the given leftLine using the rightTableLookup
	 * @param leftLine
	 *             The row to join the right line on
	 * @param leftKeyColumnNumber
	 *             The number of the column that will be used as key in the left line
	 * @param defaultRightLine
	 *             Specify a default entry to be put if a matching entry is not found in the right table (LEFT JOIN).<br>
	 *             Cannot be empty or null if handleDefaultRightLine is <code>true</code>.<br>
	 *             Leave empty or null if you want an INNER JOIN when handleDefaultRightLine is <code>false</code>.
	 * @param wbemKeyType
	 *             Whether the key is in the form of a WBEM path, which needs to be sorted before searching for matches
	 * @param caseInsensitive
	 *             Whether the matching is done case insensitive
	 * @param handleDefaultRightLine
	 *             If <code>true</code> then LEFT JOIN is performed when the entry is not found in the right table.
	 * @param rightTableLookup
	 *             The right table when each row is indexed by the right key
	 * @return a line representation, i.e. {@link List} of {@link String} values
	 */
	protected static List<String> joinLine(final List<String> leftLine, final int leftKeyColumnNumber,
										   final List<String> defaultRightLine, final boolean wbemKeyType, final boolean caseInsensitive,
										   final boolean handleDefaultRightLine, final Map<String, List<String>> rightTableLookup) {

		// Extract the key column from the left line
		String leftKey = getKey(leftLine, leftKeyColumnNumber, wbemKeyType, caseInsensitive);

		// Retrieve the right line that matches with the value of the left key
		final List<String> rightLine = rightTableLookup.get(leftKey);

		// Different cases, whether we have a default right line or not
		if (handleDefaultRightLine) {
			// If we have a default right line, it means that each line of the left table
			// will be added
			// sometimes with the matching right line from the right table, and sometimes
			// with the default right line

			return Stream.concat(
							leftLine.stream(),
							rightLine != null ? rightLine.stream() : defaultRightLine.stream())
					.collect(Collectors.toList());

		} else {
			// No default right line, which means that we first need to check whether the
			// key is present in the right table
			if (rightLine != null) {
				return Stream.concat(leftLine.stream(), rightLine.stream())
						.collect(Collectors.toList());
			}

			return Collections.emptyList();
		}

	}

	/**
	 * Sort alphabetically the key properties of a WBEM object path so that we can match them easily while joining the 2 tables
	 *
	 * @param wbemPath
	 *             WBEM objectPath to be "normalized"
	 * @return Re-ordered WBEM path
	 */
	private static String normalizeWbemReference(String wbemPath) {

		// wbemPath is in the form below:
		// class.key1="value1",key2="value2"
		// Split the WBEM path to have the class and the list of key properties
		int dotIndex = wbemPath.indexOf('.');
		if (dotIndex < 1) {
			// This is not the expected format, let's quit without touching that string
			return wbemPath;
		}
		String className = wbemPath.substring(0, dotIndex);
		String keyProperties = wbemPath.substring(dotIndex + 1);

		// Split the key properties in an array to be sorted
		// We will split when we find ", which marks the end of the value of a key
		// However, to avoid deleting the double quote, we will replace all occurrences of ", by "\n
		// and then split on \n
		keyProperties = keyProperties.replaceAll("\",", "\"\n");
		String[] keyPropertyArray = keyProperties.split("\n");

		// null?
		if (keyPropertyArray == null) {
			// Return wbemPath unchanged
			return wbemPath;
		}

		// Sort the array of key properties
		Arrays.sort(keyPropertyArray);

		// Return a string with the key properties in the right order
		StringBuilder result = new StringBuilder(className).append(".");
		for (String keyPropertyItem : keyPropertyArray) {
			result.append(keyPropertyItem).append(".");
		}
		result.delete(result.length() - 1, result.length());
		return result.toString();
	}
}

