# CFPB_Complaints_Analysis
## Understanding Financial Complaint Outcomes: A Data-Driven Approach

This project aims to analyze financial product complaints, predict their outcomes, and uncover insights into consumer behavior and possible improvements for financial services.

Table of Contents

1. [Installation](#installation)
2. [Dataset](#dataset)
3. [Usage](#usage)
4. [Contributing](#contributing)

# Installation

To run this project, you will need to have Python, Spark, and Java installed on your machine.

### Python

You can download and install Python from the official website: https://www.python.org/downloads/

### Spark

You can download and install Spark from the official website: https://spark.apache.org/downloads.html
or You can download it [here](http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz) and then extract the files with the following command:

#!/bin/bash

# Download Spark
wget -q http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz

# Extract Spark
tar xf spark-3.1.1-bin-hadoop3.2.tgz

# Set the SPARK_HOME environment variable
export SPARK_HOME=$(pwd)/spark-3.1.1-bin-hadoop3.2

# Add Spark's bin directory to the PATH
export PATH=$PATH:$SPARK_HOME/bin


### Java

Java installation can be achieved via SDKMAN!. Install SDKMAN! by following the instructions on their website: https://sdkman.io/install

After SDKMAN! is installed, a new Java version can be installed by running:

- sdk install java 11.0.3.hs-adpt
  
Then, the installed version can be set as default with:

- sdk use java 11.0.3.hs-adpt

Check the installed version with:

- java -version

**You'll also need to download a couple of JAR files:**

- wget https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.32.0/spark-bigquery-with-dependencies_2.12-0.32.0.jar
  
- wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

Then, set your `JAVA_HOME` and `SPARK_HOME` environment variables. Here's how you might do it on Unix-based systems:

- export JAVA_HOME=/path/to/your/java/home
  
- export SPARK_HOME=/path/to/your/spark/home

Remember to replace `/path/to/your/java/home` and `/path/to/your/spark/home` with the actual paths on your system.

### Python Dependencies

Lastly, you'll need to install several Python packages. You can do this by navigating to the project root directory in your terminal and running:

- pip install -r requirements.txt

#### Note versions that I am using are:
- **openjdk version "11.0.20" 2023-07-18**
- **OpenJDK Runtime Environment Homebrew (build 11.0.20+0)**
- **OpenJDK 64-Bit Server VM Homebrew (build 11.0.20+0, mixed mode)**
- **Python 3.9.13**
- **Spark version 3.1.1**


# Dataset
We use the Consumer Financial Protection Bureau (CFPB) Complaint Database, hosted on Google's BigQuery public datasets. This 2.15GB dataset consists of over 3.4 million rows of data on consumer complaints related to financial products and services reported to the CFPB from 2011 to 2023.

# Usage
After you've installed all the necessary prerequisites, you can run the application with the following command:
python app.py

# Contributing
Contributions are welcome. Please submit a Pull Request with any improvements or bug fixes.
