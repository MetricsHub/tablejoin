# TableJoin Java Client
The TableJoin utility is designed for joining tables expressed in CSV or structed as list of String lists. 
It enhances the flexibility of data processing by allowing developers to perform joint operations on specified tables.

# How to run the TableJoin Client inside Java

Add TableJoin in the list of dependencies in your [Maven **pom.xml**](https://maven.apache.org/pom.ht):

```
<dependencies>
	<!-- [...] -->
	<dependency>
		<groupId>org.sentrysoftware</groupId>
		<artifactId>tablejoin</artifactId>
		<version>${project.version}</version>
	</dependency>
</dependencies>
```

Invoke the TableJoin Client:

```
	public static void main(String[] args) {

        String leftTableCSV = "a1,b1,c1\na2,b2,c2\na3,b3,c3";
        String rightTableCSV = "a1,5\na2,8\na3,3";

        int leftKeyColumnNumber = 1;
        int rightKeyColumnNumber = 1;

        String separator = ",";
        String defaultRightLine = null;

        String result = TableJoinClient.join(
			leftTableCSV, 
			rightTableCSV, 
			leftKeyColumnNumber, 
			rightKeyColumnNumber,
            separator, 
			defaultRightLine, 
			false, 
			false
		);

        System.out.println("Join Result :");
        System.out.println(result);
    }
```
