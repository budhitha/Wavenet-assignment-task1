# WaveNet Task1

## Configurations

Please keep application.properties file inside run directory and Update the source csv file directory as well.

## Running
- To create uber jar
    - ```sbt clean assembly```
    
- Spark submit
    - ```spark-submit --class com.blp.wntask.Task1 --master local[*] path/to/jar``` 
```