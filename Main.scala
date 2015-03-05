/**
 * Created by Grace & Gabriela.
 */

import scala.collection.mutable.Map
import scala.collection.mutable.PriorityQueue
import scala.io.Source

object Main {

  class Field[T] (var value: T)

  def isNewCategory(myList: List[Data], x: String, field: Field[Int]): Unit = {
    if (!myList.isEmpty){
      if (myList.head.getCategory() == x) {
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

  def HyperCalculate(popSize: Int, catsizeX:Int,category:String, numK:Int, topK:Array[Data]): Float = {
    val numcatTopK= CatXinTopK(category, topK)
    var caltop:Float = Combination(catsizeX) * Combination(popSize-catsizeX) * Combination(popSize-numK) * Combination(numK)
    var calbot: Float = Combination(popSize) * Combination(numcatTopK) * Combination(catsizeX-numcatTopK) * Combination(numK-numcatTopK)*Combination(popSize-catsizeX-numK+numcatTopK)
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
    if (x<0)
      return -1
    else
      return recurCom(x,1)

  }
  def recurCom(x:Int, total:Int):Int = {
    if (x>1)
      recurCom(x-1, total*x)
    else
      return total
  }


//look at List on web and find length func name
  def merge_sort(m: List[Data]): List[Data] = {
   if (m.length <= 1)
     return m
   var (left, right) = m.splitAt(m.length / 2)

   left = merge_sort(left)
   right = merge_sort(right)

   merge(left, right)
  }

  def merge(left: List[Data], right: List[Data]):List[Data] = {
    var result= List[Data]()
    var l= left; var r=right
    while (!l.isEmpty && !r.isEmpty) {
      if (l.head.getScore() <= r.head.getScore() ){
        result= l.head :: result
        l = l.tail
      } else {
        result = r.head :: result
        r = r.tail
      }
    }

    while (!l.isEmpty){
      result= l.head :: result
      l = l.tail
     }
    while (!r.isEmpty){
      result = r.head :: result
      r = r.tail
    }
    return result
  }




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

    var HyperMap : Map[String,Float] = Map()

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

