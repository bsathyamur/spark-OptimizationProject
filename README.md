# Spark Optimization Mini Project

## Goal: To optimize the existing spark code for better performance tuning by analyzing the below factors:

1. By picking the right operators
2. Reduce the number of shuffles and the amount of data shuffled
3. Tuning Resource Allocation
4. Tuning the Number of Partitions
5. Reducing the Size of Data Structures
6. Choosing Data Formats

## Performance tuning done

1. repartition based on month column since we need to finally group the count based on question_id and month. This will avoid unnecessary shuffles
    
answers_month = answers_month.repartition(col("month"))
    
2. Enabled AQE (Adaptive Query Execution) to True

## Performace outcomes

The spark code is optimized and execution completes within 2.5s instead of 5.3 seconds as shown below:

![img1](https://github.com/bsathyamur/spark-OptimizationProject/blob/main/performance.png)
