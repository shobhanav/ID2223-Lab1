package se.kth.spark.lab1.task6

import org.apache.spark.ml.linalg.{Matrices, Vector, Vectors}

object VectorHelper {
  def dot(v1: Vector, v2: Vector): Double = {    
    return (v1.toArray,v2.toArray).zipped.map((x1,x2)=>x1*x2).sum
  }

  def dot(v: Vector, s: Double): Vector = {    
    return Vectors.dense(v.toArray.map(a => a*s))
  }

  def sum(v1: Vector, v2: Vector): Vector = {
    return Vectors.dense((v1.toArray,v2.toArray).zipped.map((x1,x2)=>x1 + x2))
  }

  def fill(size: Int, fillVal: Double): Vector = {
    return Vectors.dense(Array.fill[Double](size)(fillVal))
  }
}