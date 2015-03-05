/**
 * Created by Gabriela & Grace.
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

  def putStuffInQueue(myQueue: PriorityQueue[Data], k: Int, counter: Field[Int], myData: Data) : PriorityQueue[Data] = {
    if (counter.value < k) {
      myQueue.enqueue(myData)
      counter.value = counter.value + 1
      myQueue
    }
    else {
      if (myData.getScore() > myQueue.min(Ordering.by(greaterThan)).getScore() ){
        var tempQueue = myQueue.reverse
        tempQueue.dequeue()
        tempQueue = tempQueue.reverse
        tempQueue.enqueue(myData)
        tempQueue
      }
      else {
        myQueue
      }
    }
  }

 /* def getTopK(myQueue: PriorityQueue[Data], k: Int, arr:Array[Data], index: Field[Int]) : Unit = {
    var temp = k;
    if (k > 0){
      temp = temp-1
      arr(index.value) = myQueue.head
      index.value = index.value + 1
      getTopK(myQueue.tail, temp, arr, index)
    }
  }*/

  //Hypergeometric distribution
  /*
  def HyperCalculate(popSize: Int, catsizeX:Int,category:String, numK:Int, topK:Array[Data]): Unit = {
    val numcatTopK= CatXinTopK(category, topK)
    var cal= Combination(catsizeX)*Combination(popSize-catsizeX)
    var cal2= cal/(Combination(numcatTopK)*Combination(catsizeX-numcatTopK)*Combination(numK-numcatTopK)*Combination(popSize-catsizeX-numK+numcatTopK)  )
  }
*/
  /*def CatXinTopK(category: String, topK:Array[Data]):Unit = {
    var count=0;
    for ( i <- 0 until topK.length) {
      if(topK(i).getCategory()==category)
        count=count+1
    }
    return count
  }*/
  //returns combination of number
  def Combination(x: Int):Unit= {
    if (x<1)
      return -1;
    else
      recurCom(x,1);
  }
  def recurCom(x:Int, total:Int):Unit = {
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
    var counter = new Field(0)
    var categoryCounter: Option[Int] = None
    var queue = new PriorityQueue[(Data)]()(Ordering.by(greaterThan))

    //number of top k
    val k = args(1).toInt

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
      queue = putStuffInQueue(queue, k, counter, dataInst)
      N = N+1
    }


    for (i<- 0 until k){
      println (queue.max(Ordering.by(greaterThan)).getScore())
      queue.dequeue()
    }

    myMap.keys.foreach {i =>
      println("Keys: " + i)
      println("value: " + myMap(i))
    }



  }

}
