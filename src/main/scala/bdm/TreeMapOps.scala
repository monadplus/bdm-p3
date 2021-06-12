package bdm

import scala.collection.immutable.TreeMap

object TreeMapOps {
  implicit class TreeMapOps[K, V](tree: TreeMap[K, V]) {
    def unionWith[V1 <: V](other: TreeMap[K, V])(merge: (V, V) => V): TreeMap[K, V] = {
      def go(acc: TreeMap[K, V], kv: (K, V)): TreeMap[K, V] = {
        val (k, v1) = kv
        acc.get(k) match {
          case None     => acc + kv
          case Some(v2) => acc.updated(k, merge(v1, v2))
        }
      }

      other.foldLeft(tree)(go)
    }
  }
}
