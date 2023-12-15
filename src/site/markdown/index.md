# TableJoin Utility
The TableJoin utility is designed for joining tables expressed in CSV or structured as list of String lists.
It enhances the flexibility of data processing by allowing developers to perform joint operations on specified tables.
# How to run the TableJoin Utility inside Java

Add TableJoin in the list of dependencies in your [Maven **pom.xml**](https://maven.apache.org/pom.html):

```xml
<dependencies>
	<!-- [...] -->
	<dependency>
        <groupId>${project.groupId}</groupId>
        <artifactId>${project.artifactId}</artifactId>
		<version>${project.version}</version>
	</dependency>
</dependencies>
```

Invoke the TableJoin Utility:

```java
    public static void main(String[] args) throws Exception {
    
        final String leftTableCSV = "a1,b1,c1\na2,b2,c2\na3,b3,c3";
        final String rightTableCSV = "a1,5\na2,8\na3,3";
        
        final int leftKeyColumnNumber = 1;
        final int rightKeyColumnNumber = 1;
        
        final String separator = ",";
        final String defaultRightLine = null;
        
        final String result = TableJoinClient.join(
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
