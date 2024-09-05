# 22MIS1120---M-THANUSHA
# "Analyzing Literacy and Population Trends Across Indian Cities"

## Overview

This involves analyzing city data using Hadoop MapReduce. It processes city population statistics and literacy rates to generate aggregated results.

## Getting Started

To get started, you'll need to set up Hadoop and Java. 

### Prerequisites

- Hadoop 3.x
- Java 8 or higher

### Running the Project

1. **Compile the Java Code:**

   ```bash
   javac -classpath `hadoop classpath` -d bin src/CityDataMapReduce.java

2. **Create a JAR File:**

bash
Copy code
jar -cvf CityDataMapReduce.jar -C bin/ .

3. **Run the Hadoop Job:**

bash
Copy code
hadoop jar CityDataMapReduce.jar CityDataMapReduce /path/to/input /path/to/output
