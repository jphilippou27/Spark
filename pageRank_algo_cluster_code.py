from __future__ import division
import re
import ast
import time
import numpy as np
import pandas as pd



# start Spark Session
from pyspark.sql import SparkSession
app_name = "PageRank_notebook"
master = "local[*]"
spark = SparkSession\
        .builder\
        .appName(app_name)\
        .master(master)\
        .getOrCreate()
sc = spark.sparkContext

#Load data
#testRDD = sc.textFile('gs://storagehw5/data/test_graph.txt') #cloud location
testRDD = sc.textFile('data/test_graph.txt') #local docker HDFS location

# job to initialize the graph
def initGraph(dataRDD):
    """
    Spark job to read in the raw data and initialize an 
    adjacency list representation with a record for each
    node (including dangling nodes).
    
    Returns: 
        graphRDD -  a pair RDD of (node_id , (score, edges))
        
    """
    
    dataRDD = testRDD
    #  helper functions 
    def parse(graph_links):
        #makes the full list of nodes
        link_cnt =0
        link_dict = ast.literal_eval(graph_links[1])
        for key, value in link_dict.items():
            link_cnt += 1
            yield (key, value)
        yield (graph_links[0], link_cnt)
        
    def post_join_process(graph_dict):
        #the edges need to be a dict, parsing accordingly
        edges = graph_dict[1][1]
        if edges == None: #if a dangling node, create an empty 'set' for it
            return (graph_dict[0], (float(N2.value), dict()))#graph_dict[1][0],{})
        else: #otherwise return the dict of edges
            return (graph_dict[0], (float(N2.value), ast.literal_eval(graph_dict[1][1]))) #graph_dict[1][0],ast.literal_eval(graph_dict[1][1]))
    
    print(dataRDD.collect())

    graphRDD = dataRDD.map(lambda line: line.split('\t')) \
                        .flatMap(parse) \
                        .reduceByKey(lambda a, b: int(a) + int(b)) 
    graph_count = graphRDD.count()
    
    
    N = float(1/graph_count)
    N2 = sc.broadcast(N) 
    
    #joining list of nodes to the edges and assigning baseline probability
    temp = dataRDD.map(lambda line: (line.split('\t')[0],line.split('\t')[1]))
    
    graphRDD = graphRDD.leftOuterJoin(temp) \
                        .map(post_join_process) \
                        .cache()
    return graphRDD

# FloatAccumulator class 

from pyspark.accumulators import AccumulatorParam

class FloatAccumulatorParam(AccumulatorParam):
    """
    Custom accumulator for use in page rank to keep track of various masses.
    
    """
    def zero(self, value):
        return value
    
# job to run PageRank 
def runPageRank(graphInitRDD, alpha = 0.15, maxIter = 10 , verbose = True):
    """
    Spark job to implement page rank
    Args: 
        graphInitRDD  - pair RDD of (node_id , (score, edges))
        alpha         - (float) teleportation factor
        maxIter       - (int) stopping criteria (number of iterations)
        verbose       - (bool) option to print logging info after each iteration
    Returns:
        steadyStateRDD - pair RDD of (node_id, pageRank)
    """
    # teleportation:
    a = sc.broadcast(alpha)
    
    # damping factor:
    d = sc.broadcast(1-a.value)
    
    # initialize accumulators for dangling mass & total mass
    mmAccum = sc.accumulator(0.0, FloatAccumulatorParam())
    totAccum = sc.accumulator(0.0, FloatAccumulatorParam())
    
    N = sc.broadcast(graphInitRDD.count())
    
    # write your helper functions here
    def parse(line):
        #reads in each record and redistributes the node's current score to each of its neighbors
        key, value, dict_edges = line[0], line[1][0], line[1][1]
        total_edge_cnt = 0
        edges_list = []
        for edge_key, edge_cnt in line[1][1].items():
            total_edge_cnt += edge_cnt
            edges_list.append(edge_key)
        #find danglers and increment 
        if len(line[1][1].items()) != 0:
            new_score = (line[1][0])/total_edge_cnt
            #redistribute the node's current score
            for item in range(1,total_edge_cnt+1):
                if len(edges_list)<=item:
                    sub_key = edges_list[0]
                elif total_edge_cnt != 0:
                    sub_key = edges_list[(item-1)]
                yield(sub_key, (float(new_score),line[1][0])) #new line
                #yield(sub_key, (float(new_score), {}))
            #yield(key,( 0, line[1][1]))
        dangler_m = (dangling_mass.value / N.value)
        yield(key, (dangler_m, line[1][1]))
        
    def find_danglers(line): #, accumulator):
        #uses an accumulator to add up the dangling node mass 
        #Don't forget to reset this accumulator after each iteration!)
        key, value, dict_edges = line[0], line[1][0], line[1][1]
        #find danglers and increment 
        if len(line[1][1].items()) == 0:
            mmAccum.add(value)
            
    def calc_total_mass(line):
        #total_mass = a*line[1][0] +(d)*[DanglingNODE/N]+[redistributed_probability]
        key, p, dict_edges = line[0], line[1][0], line[1][1]
        new_P = (a.value*(1/N.value)) + (d.value *p)
        return (key, (new_P,dict_edges))
    def qc_sum(line):
        key, value = line[0],line[1][0]
        totAccum.add(value)
        
    def merge_dicts2(x,y):
        #merging the dictionaries for the older python version
        if (type(x) == dict) & (type(y) == dict):
            y.update(x)
            return (y)
        elif type(x) == dict:
            return (x)
        else:
            return (y)
    
    # main Spark Job 
    dataRDD = graphInitRDD
    for i in range(1,maxIter+1):
        #DANGLING MASS WORK    
        dataRDD.foreach(find_danglers)
        print("dangler mass to redistribute", mmAccum) 
        #print("dataRDD check", dataRDD.collect())
        #work around for this error: Exception: Accumulator.value cannot be accessed inside tasks
        dangling_mass = sc.broadcast(mmAccum.value)
        
        reDistributeRDD = dataRDD.flatMap(parse) \
                         .reduceByKey(lambda x,y: (x[0]+y[0],merge_dicts2(x[1],y[1])))#Old way --- shout out to Yulia for the tip! .reduceByKey(lambda a, b: a + b) ---from python2.7 vs 3 issues (I have since learned how to change the node's python setup  :)
        #print("new p", reDistributeRDD.collect())
        reDistributeRDD.foreach(qc_sum)
        print("totalAccumulator QC: ", totAccum)
        
        reDistributeRDD = reDistributeRDD.map(calc_total_mass)#.collect()
        
        #print("reDistributeRDD.collect())
        steadyStateRDD = reDistributeRDD.map(lambda data: (data[0], data[1][0]))
        #print(steadyStateRDD2.collect())
        #print("post join qc", steadyStateRDD.collect())

        #reset accumulators between transitions
        mmAccum = sc.accumulator(0.0, FloatAccumulatorParam())
        totAccum = sc.accumulator(0.0, FloatAccumulatorParam())
        dataRDD = reDistributeRDD
        
    return steadyStateRDD

###################
#Apply code
###################

# run PageRank on the test graph 
nIter = 20
#testGraphRDD = initGraph(testRDD)

# run Spark job on the test graph 
start = time.time()
testGraph = initGraph(testRDD)#.collect()
print('... test graph initialized in ',time.time() - start, 'seconds.')
print("post q7", testGraph.collect())

start = time.time()
test_results = runPageRank(testGraph, alpha = 0.15, maxIter = nIter, verbose = False)
print('...trained', nIter ,'iterations in ',time.time() - start , 'seconds.')
print('Top 20 ranked nodes:')
print(test_results.takeOrdered(20, key=lambda x: - x[1]))