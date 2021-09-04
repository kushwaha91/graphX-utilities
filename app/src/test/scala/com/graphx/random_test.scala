package com.graphx
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object random_test {
  def main(args: Array[String]): Unit = {
    println("Hi")
    val spark = SparkSession.builder
                            .appName("graphx_demo")
                            .master("local[*]")
                            .getOrCreate
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    val graph : Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    //filterVertices(graph)
    describeGraph(graph)
    graphOperations(graph)
  }

  def filterVertices(graph: Graph[(String, Int), Int]) = {
    val filteredGraph = graph.vertices.filter{
      case (id, (name, age)) => age > 30 }
    filteredGraph.foreach(println)
  }

  def describeGraph(graph: Graph[(String, Int), Int]) = {
    println("#####GRAPH INFO#####")
    println(graph.numEdges)
    println(graph.numVertices)
    println("In degrees")
    graph.inDegrees.foreach(println)
    println("Out degrees")
    graph.outDegrees.foreach(println)
    println("Degrees")
    graph.degrees.foreach(println)
    val triplets = graph.triplets
    println("Triplets")
    triplets.foreach(println)
  }

  def graphOperations(graph: Graph[(String, Int), Int]) = {
    val subgraph = graph.subgraph(vpred = (v,d) => d._2 >30)
    describeGraph(subgraph)
    val subgraph2 = graph.subgraph(epred = (v) => v.dstAttr._2 < 30 && v.dstAttr._2 > 30)
    describeGraph(subgraph2)

  }
}
