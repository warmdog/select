


import org.apache.spark.graphx._
import org.apache.spark.util.collection.OpenHashSet

import scala.collection.mutable
import scala.collection.mutable.{ArraySeq, ListBuffer}
import scala.reflect.ClassTag

/**
  * Compute the number of triangles passing through each vertex.
  *
  * The algorithm is relatively straightforward and can be computed in three steps:
  *
  * <ul>
  * <li> Compute the set of neighbors for each vertex</li>
  * <li> For each edge compute the intersection of the sets and send the count to both vertices.</li>
  * <li> Compute the sum at each vertex and divide by two since each triangle is counted twice.</li>
  * </ul>
  *
  * There are two implementations.  The default `TriangleCount.run` implementation first removes
  * self cycles and canonicalizes the graph to ensure that the following conditions hold:
  * <ul>
  * <li> There are no self edges</li>
  * <li> All edges are oriented (src is greater than dst)</li>
  * <li> There are no duplicate edges</li>
  * </ul>
  * However, the canonicalization procedure is costly as it requires repartitioning the graph.
  * If the input data is already in "canonical form" with self cycles removed then the
  * `TriangleCount.runPreCanonicalized` should be used instead.
  *
  * {{{
  * val canonicalGraph = graph.mapEdges(e => 1).removeSelfEdges().canonicalizeEdges()
  * val counts = TriangleCount.runPreCanonicalized(canonicalGraph).vertices
  * }}}
  *
  */
object NewTriangleCount {
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Array[Array[String]], ED] = {
    // Transform the edge data something cheap to shuffle and then canonicalize
    val canonicalGraph = graph.mapEdges(e => true).removeSelfEdges().convertToCanonicalEdges()
    // Get the triangle counts
    val counters = runPreCanonicalized(canonicalGraph).vertices
    // Join them bath with the original graph
    val a =graph.outerJoinVertices(counters) { (vid, _, optCounter: Option[Array[Array[String]]]) =>
      optCounter.getOrElse(null)
    }
    canonicalGraph.edges.foreach(println(_))
    a
  }


  def runPreCanonicalized[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Array[Array[String]], ED] = {
    // Construct set representations of the neighborhoods

    val nbrSets: VertexRDD[OpenHashSet[VertexId]] =
      graph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new OpenHashSet[VertexId](nbrs.length)
        var i = 0
        while (i < nbrs.length) {
          // prevent self cycle
          if (nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }
    // join the sets with the graph
    val setGraph: Graph[OpenHashSet[VertexId], ED] = graph.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }

    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(ctx: EdgeContext[OpenHashSet[VertexId], ED, Int]) {
      val (smallSet, largeSet) = if (ctx.srcAttr.size < ctx.dstAttr.size) {
        (ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstAttr, ctx.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != ctx.srcId && vid != ctx.dstId && largeSet.contains(vid)) {
          counter += 1
        }
      }
      ctx.sendToSrc(counter)
      ctx.sendToDst(counter)
    }
    def edgeFunc1(ctx: EdgeContext[OpenHashSet[VertexId], ED, Array[Array[String]]]) {
      val (smallSet, largeSet) = if (ctx.srcAttr.size < ctx.dstAttr.size) {
        (ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstAttr, ctx.srcAttr)
      }
      val iter = smallSet.iterator
      var resSrc  =Array[Array[String]]()
      var resDst  =Array[Array[String]]()
      //      var resDst = List[mutable.HashMap[VertexId,List[VertexId]]]()
      //      var resSrc = List[mutable.HashMap[VertexId,List[VertexId]]]()
      //val map = mutable.HashMap[VertexId, ]()
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != ctx.srcId && vid != ctx.dstId && largeSet.contains(vid)) {
          val min1 = Math.min(ctx.srcId,vid)
          val min2 = Math.min(ctx.dstId,vid)
          resDst = resDst:+ (if (ctx.srcId<vid)  Array(ctx.srcId.toString,vid.toString) else  Array(vid.toString,ctx.srcId.toString))
          resSrc = resSrc:+ (if (ctx.dstId<vid)  Array(ctx.dstId.toString,vid.toString) else  Array(vid.toString,ctx.dstId.toString))
        }
      }
      ctx.sendToSrc(resSrc)
      ctx.sendToDst(resDst)
    }
    // compute the intersection along edges
    val counters:VertexRDD[Array[Array[String]]]= setGraph.aggregateMessages[Array[Array[String]]](edgeFunc1, (a,b) =>{
      val res = (a++b)
      res
      //      res.toList.groupBy(x=>{
      //        val a = x(0).hashCode
      //        val b =x(1).hashCode
      //      }).map(_._2.head)
    })
    // Merge counters with the graph and divide by two since each triangle is counted twice
    graph.outerJoinVertices(counters) { (_, _, optCounter: Option[Array[Array[String]]]) =>
      val dblCount = optCounter.getOrElse(null)
      // This algorithm double counts each triangle so the final count should be even
      //require(dblCount % 2 == 0, "Triangle count resulted in an invalid number of triangles.")
      //dblCount / 2
      dblCount
    }
  }

}
