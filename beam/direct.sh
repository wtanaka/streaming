mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount -Dexec.args="--inputFile=pom.xml --output=counts" -Pdirect-runner
