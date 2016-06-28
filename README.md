# Benchmark MinHash+LSH algorithm on Spark
* Insight Data Engineering Fellow Project

## Motivation: Explore approximate neighbor algorithms

This project is motivated by speeding up neighbor searching for recommendation/classification algorithms. Even for the most straightforward algorithm, KNN, the naive brute-force implementation takes O(n^2)*O(similarity calculation). In the graduate school, I learned to implement LSH + Random Projection approach to speed up KNN with cosine similarity. For this project, I'm exploring another variant of LSH algorithm: MinHash + LSH for speeding up neighbor search wrt Jaccard Similarity ("Mining massive datasets", Rajaraman and Leskovec). Jaccard Similarity is really useful for finding similar documents (e.g. plagirism) and sequences. I'm interested in implementing and benchmarking this algorithm in both batch and real-time mode on the distirbuted system (Spark, Kafka and Redis).

## MinHash + LSH Implementation

First of all, let's define our task is to recommend posts based on Jaccard Similarity of texts. Now suppose we have three posts in the pool:
![alt tag](pics/minhash_.png)

![alt tag](pics/lsh_bands_.png)

## Pipeline
![alt tag](pics/pipeline.png)

## Evaluation
## Limitations & Conclusion






