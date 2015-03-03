/**
 * Created by Grace & Gabriela.
 */

import scala.collection.mutable.{Map, PriorityQueue}
import scala.io.Source

object Main {

  class Field[T] (var value: T)

  def isNewCategory(myList: List[Data], x: String, field: Field[Int]): Unit = {
    if (!myList.isEmpty){
      if (myList.head.getCategory() == x){
        field.value = 0
      }
      isNewCategory(myList.tail, x, field)
    }
  }

  def greaterThan(x: Data): Double = {
    x.getScore()
  }

  /*def printQueue(myQueue: PriorityQueue[Data]): Unit ={
    if (!myQueue.isEmpty){
      println(myQueue.head.getScore())
      printQueue(myQueue.tail)
    }
  }*/

  def putStuffInQueue(myQueue: PriorityQueue[Data], myList: List[Data]) : Unit = {
    if (!myList.isEmpty){
      myQueue.enqueue(myList.head)
      putStuffInQueue(myQueue, myList.tail)
    }
  }

  def getTopK(myQueue: PriorityQueue[Data], k: Int, arr:Array[Data], index: Field[Int]) : Unit = {
    var temp = k;
    if (k > 0){
      temp = temp-1
      arr(index.value) = myQueue.head
      index.value = index.value + 1
      getTopK(myQueue.tail, temp, arr, index)
    }
  }

  //Hypergeometric distribution

  def HyperCalculate(popSize: Int, catsizeX:Int,category:String, numK:Int, topK:Array[Data]): Int = {
    val numcatTopK= CatXinTopK(category, topK)
    var caltop= Combination(catsizeX) * Combination(popSize-catsizeX) * Combination(popSize-numK) * Combination(numK)
    var calbot= Combination(popSize) * Combination(numcatTopK) * Combination(catsizeX-numcatTopK) * Combination(numK-numcatTopK)*Combination(popSize-catsizeX-numK+numcatTopK)
    return caltop/calbot
  }

  def CatXinTopK(category: String, topK:Array[Data]):Int = {
    var count=0;
    for ( i <- 0 until topK.length) {
      if(topK(i).getCategory()==category)
        count=count+1
    }
    return count
  }
  //returns combination of number
  def Combination(x: Int):Int= {
    if (x<1)
      return -1;
    else
      recurCom(x,1);
  }
  def recurCom(x:Int, total:Int):Int = {
    if (x>1)
      recurCom(x-1, total*x)
    else
      return total;
  }


/*
  def merge_sort(List m) {
   if length(m) <= 1
     return m
   var (left, right) = m.splitAt(length(m)/2)

   left = merge_sort(left)
   right = merge_sort(right)

   merge(left, right)
  }

  def merge(left, right) {
    var list result
    while (!left.isEmpty and !right.isEmpty) {
      if first(left) <= first(right)
        append first(left) to result; left = rest(left)
      else
        append first(right) to result; right = rest(right)
    }

    while !left.isEmpty{
      append first(left) to result
      left = rest(left)
     }
    while !right.isEmpty{
      append first(right) to result
      right = rest(right)
    }
    return result
  ￼}
  */

  def main(args: Array[String]) : Unit = {

    val fileName = args(0)
    var data = " "
    var dataList : List[Data] = List()
    var categoryNum = new Field(1)
    var categoryCounter: Option[Int] = None
    //pop size
    var N = 0
    var myMap : Map[String,Int] = Map()
    for (line <- Source.fromFile(fileName).getLines()){
      var dataInst = new Data()
      data = line.split(" ")(0)
      dataInst.setScore(data.toDouble)
      data = line.split(" ")(1)
      dataInst.setCategory(data)
      isNewCategory(dataList, data, categoryNum);

      dataList = dataList :+ (dataInst)
      if (categoryNum.value == 1){
        myMap += (data -> 1)
      }
      else {
        myMap.update(data, myMap(data)+1)
      }
      categoryNum.value = 1
      N = N+1
    }

    val queue = new PriorityQueue[(Data)]()(Ordering.by(greaterThan))

    putStuffInQueue(queue, dataList)
    //number of top k
    val k = args(1).toInt

    var topK = new Array[Data](k)


    var index = new Field(0)

    getTopK(queue, k, topK, index)

    var HyperMap : Map[String,Int] = Map()

    myMap.keys.foreach { i =>
      HyperMap += (i -> HyperCalculate(N,myMap(i),i,k,topK))
    }

    for (i <-0 until topK.length){
      println(topK(i).getScore())
    }

    myMap.keys.foreach {i =>
      println("Keys: " + i)
      println("value: " + myMap(i))
      println("hyperval: " + HyperMap(i))
    }



  }

}

