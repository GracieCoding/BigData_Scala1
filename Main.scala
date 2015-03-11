/**
 * Created by Gabriela & Grace.
 */

import scala.collection.mutable.{Map, PriorityQueue}
import scala.io.Source
import scala.actors.Actor
import scala.actors.Actor._

object Main {

  class Field[T] (var value: T)

  //Sees if a category is new or not

  def isNewCategory(myList: List[Data], x: String): Boolean = {
    if (!myList.isEmpty){
      println(myList.head.getCategory() + "compare to " + x)
      if (myList.head.getCategory() == x) {
        false
      }
      else {
        isNewCategory(myList.tail, x)
      }
    }
    else{
      true
    }
  }

  //Function to order the queue by
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

  class HypActor extends Actor {
    def act {
      while(true) {
        receive {
          case (x: Int, y: Int, z: String, w: Int, v: Array[Data]) =>
            sender ! ( HyperCalculate(x, y, z, w, v) , z )
            exit()
          case _ => println("what the fuck is this info you gave me, inside actor")
        }
      }
    }
  }

  //Hypergeometric distribution

  def HyperCalculate(popSize: Int, catsizeX:Int,category:String, numK:Int, topK:Array[Data]): BigDecimal = {
    val numcatTopK= CatXinTopK(category, topK)

    var caltop: BigDecimal = Combination(catsizeX) * Combination(popSize-catsizeX) * Combination(popSize-numK) * Combination(numK)
    var calbot: BigDecimal = Combination(popSize) * Combination(numcatTopK) * Combination(catsizeX-numcatTopK) * Combination(numK-numcatTopK)*Combination(popSize-catsizeX-numK+numcatTopK)

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
  def Combination(x: Int):BigDecimal= {
    if (x<0)
      return -1
    else
      return recurCom(x,1)
  }
  def recurCom(x:Int, total:Int):BigDecimal = {
    if (x>1)
      recurCom(x-1, total*x)
    else
      return total
  }


  //look at List on web and find length func name

  def merge_sort(m: List[BData]): List[BData] = {
    if (m.length <= 1)
      return m
    var (left, right) = m.splitAt(m.length / 2)

    left = merge_sort(left)
    right = merge_sort(right)

    merge(left, right)
  }

  def merge(left: List[BData], right: List[BData]):List[BData] = {
    var result= List[BData]()
    var l= left; var r=right
    while (!l.isEmpty && !r.isEmpty) {
      if (l.head.getScore() <= r.head.getScore() ){
        result= result:+l.head
        l = l.tail
      } else {
        result = result:+r.head
        r = r.tail
      }
    }

    while (!l.isEmpty){
      result= result:+l.head
      l = l.tail
    }
    while (!r.isEmpty){
      result = result :+ r.head
      r = r.tail
    }
    return result
  }*/


  def main(args: Array[String]) : Unit = {

    val me= self
    val fileName = args(0)
    var data = " "
    var dataList: List[Data] = List()
    var categoryNum = new Field(1)
    //pop size
    var N = 0

    var mapOfNumCat : Map[String,Int] = Map()
    for (line <- Source.fromFile(fileName).getLines()){
      var dataInst = new Data()
      data = line.split(" ")(0)
      dataInst.setScore(data.toDouble)
      data = line.split(" ")(1)
      dataInst.setCategory(data)

      println("Data: " + data)
      println(isNewCategory(dataList, data))
      if (isNewCategory(dataList, data)){
        println("running")
        mapOfNumCat += (data -> 1)
      }
      else {
        println("wtf")
        mapOfNumCat.update(data, mapOfNumCat(data)+1)
      }
      dataList = dataList :+ (dataInst)

      N = N+1

    }

    val queue = new PriorityQueue[(Data)]()(Ordering.by(greaterThan))

    putStuffInQueue(queue, dataList)

    //number of top k
    val k = args(1).toInt

    var topK = new Array[Data](k)

    var index = new Field(0)

    getTopK(queue, k, topK, index)

    var HyperMap: List[BData] = List()
    var Hactors: List[HypActor] = List()

    mapOfNumCat.keys.foreach { i =>
      var actor = new HypActor
      actor.start
      Hactors = Hactors :+ actor
      actor ! (N, mapOfNumCat(i), i, k, topK)
    }

    while (HyperMap.size < mapOfNumCat.size){
      receive {
        case (x: BigDecimal, z: String) =>
          var inst = new BData()
          inst.setCategory(z)
          inst.setScore(x)
          HyperMap= HyperMap :+ inst
        case _ => println("wtf inside actor receiving ")
      }
    }

    HyperMap= merge_sort(HyperMap)

    for (i <-0 until topK.length){
      println(topK(i).getScore())
    }

    HyperMap.foreach { i =>
      println("Key: "+ i.getCategory())
      println("Value: "+ mapOfNumCat(i.getCategory()) )
      println("hyperval: "+i.getScore())
    }*/

  }

}

