package com.graphx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{lit, variance}
import spire.std.map

object HrchyReducerMock {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("graphx_demo")
      .master("local[*]")
      .getOrCreate
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // This mocks data from an organization with name, emp_id, mngr_id
    // mngr_id specifies the emp_id of a reporting manager
    // We will identify the direct and indirect reports for each employee here
    // Finding the direct would be an easy task as it can be simply identified via given mngr_id
    // Finding indirect report could be a challenge we would want to solve through graph
    import spark.implicits._
    val data = Array(
      ("Abhishek", 123, 0),
      ("Animesh", 124, 123),
      ("Seema", 125, 123),
      ("Poli", 126, 124),
      ("Goli", 127, 124),
      ("Roli", 128, 125),
      ("Chota", 129, 125),
      ("Mota", 130, 126),
      ("Lota", 131, 128))

    val dataDf = sc.parallelize(data.toSeq).toDF("name","emp_id","mngr_id")
    dataDf.printSchema
    dataDf.show(100,false)
    val emp_graph = graphGenerator(dataDf)
//    getHrchyGraph(emp_graph, 123L)
  }

  def graphGenerator(dataDf: DataFrame): Graph[(String,String),String] = {

    // Extract Vertices
    val vertices: RDD[(VertexId,(String,String))] = dataDf.select("emp_id","name")
      .withColumn("desg", lit("employee"))
      .rdd
      .map(x => (x.getInt(0).toLong, (x.getString(1),x.getString(2))))

    vertices.foreach(println)
    // Extract edges
    // Filtering non-existing edges
    val edges: RDD[Edge[String]] = dataDf.select("emp_id","mngr_id")
      .withColumn("relation", lit("direct_mgr"))
      .rdd
      .map(x=> Edge(x.getInt(0).toLong, x.getInt(1).toLong, x.getString(2)))
      .filter(x=> x.dstId.!=(0))

    edges.foreach(println)
    // Creating graph
    val emp_graph = Graph(vertices, edges)
    describeGraph(emp_graph)
    emp_graph
  }

  def describeGraph(graph: Graph[(String, String), String]) = {
    println("#####GRAPH INFO#####")
    println(graph.numEdges)
    println(graph.numVertices)
    println("In degrees")
    graph.inDegrees.foreach(println)
    println("Out degrees")
    graph.outDegrees.foreach(println)
    println("Degrees")
    graph.degrees.foreach(println)
    println("Triplets")
    graph.triplets.foreach(println)
  }

//  def getHrchyGraph(graph: Graph[(String,String),String], emp_id: Long): Graph[(String,String),String] = {
//    // provides a subgraph for all employees directly reports to employee
//    val subgraph = graph.filter(preprocess = )
//    describeGraph(subgraph)
//    subgraph
//
//  }
}
