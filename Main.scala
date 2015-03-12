/**
 * Created by Gabriela & Grace.
 */

import scala.actors.Actor
import scala.actors.Actor._
import scala.collection.mutable
import scala.collection.mutable.{Map, PriorityQueue}
import scala.io.Source

object Main {

  class Field[T] (var value: T)

  class readingInput(k: Int) extends Actor{
    var myHeap = new PriorityQueue[(Data)]()(Ordering.by(greaterThan))
    var dataInst = new Data()
    var counter = 0
    def act() = loop{
      react  {
        case (score: Double, cat: String) => {
          println("im here")
          dataInst.setScore(score)
          dataInst.setCategory(cat)
          if (isFull(counter, k)){
            val it = Iterator(myHeap)
            myHeap = myHeap.filter(it => it != myHeap.min(Ordering.by(greaterThan)))
            counter += 1
          }
          else {
            myHeap += dataInst
            counter += 1
          }
        }

        case (mapOfNumCat: Map[String, Int], category: String) =>{

        }
        /*case (false) =>{
          println("false")
          println("printing heap: ")
          myHeap += dataInst
          printQueue(myHeap)
        }
        case (true) => {

          printQueue(myHeap)
        }*/
        case ("print") =>  {
          println(myHeap.size)
          println("printing heap: ")
          printQueue(myHeap)
        }
      }
    }
  }

  //Sees if a category is new or not

  /*def isNewCategory(mapOfNumCat: Map[String,Int], x: String): Boolean = {
    if (!mapOfNumCat.isEmpty){
      mapOfNumCat.head.
      println(mapOfNumCat.head.getCategory() + "compare to " + x)
      if (mapOfNumCat.head.getCategory() == x) {
        false
      }
      else {
        isNewCategory(mapOfNumCat.tail, x)
      }
    }
    else{
      true
    }
  }*/

  //Function to order the queue by
  def greaterThan(x: Data): BigDecimal = {
    x.getScore()
  }

  def printQueue(myQueue: PriorityQueue[Data]): Unit ={
    if (!myQueue.isEmpty){
      println(myQueue.head.getScore())
      printQueue(myQueue.tail)
    }
  }



  /*def getTopK(myQueue: PriorityQueue[Data], k: Int, arr:Array[Data], index: Field[Int]) : Unit = {
    var temp = k;
    if (k > 0){
      temp = temp-1
      arr(index.value) = myQueue.head
      index.value = index.value + 1
      getTopK(myQueue.tail, temp, arr, index)
    }
  }*/



  def isFull(counter: Int, k: Int) : Boolean = {
    if (counter >= k){
      true
    }
    else {
      false
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
  }


  def main(args: Array[String]) : Unit = {

    val me= self
    val moi = self


    val fileName = args(0)
    var data = " "

    //pop size
    var N = 0

    var queue = new PriorityQueue[(Data)]()(Ordering.by(greaterThan))

    //number of top k
    val k = args(1).toInt


    val reader = actor {
      var myHeap = new PriorityQueue[(Data)]()(Ordering.by(greaterThan))
      var sum= 0
      var counter = 0
      var mapOfNumCat : Map[String,Int] = Map()

      loop {
        var dataInst = new Data()

        receive {
          case (score: Double, cat: String) =>{
            println("counter: " + counter)
            println("k: " + k)
            dataInst.setScore(score)
            dataInst.setCategory(cat)

            if (!mapOfNumCat.contains(cat)) {
              mapOfNumCat += (cat -> 1)
            }
            else {
              mapOfNumCat.update(cat, mapOfNumCat(cat)+1)
            }

            if (isFull(counter, k)){
              val it = Iterator(myHeap)
              myHeap = myHeap.filter(it => it != myHeap.min(Ordering.by(greaterThan)))
              myHeap += dataInst
              counter += 1
            }
            else {
              myHeap += dataInst
              counter += 1
            }
            println("printing heap: ")
            printQueue(myHeap)
          }
          case "heap" => moi ! myHeap
          case "map" => moi ! mapOfNumCat
        }
      }
    }

    for (line <- Source.fromFile(fileName).getLines()){

      reader ! (line.split(" ")(0).toDouble , line.split(" ")(1))
      N = N+1
    }
    var mapOfNumCat : Map[String,Int] = Map()

    reader ! "heap"
    receive {
      case(x: mutable.PriorityQueue[Data])=>{
        queue = x
      }
    }

    reader ! "map"
    receive {
      case (y: Map[String, Int]) =>{
        mapOfNumCat = y
      }
    }
    println("Printing queue: ")
    printQueue(queue)

    println("Map printing:")

    mapOfNumCat.keys.foreach{ i =>
      println("Key: " + i)
      println("value: " + mapOfNumCat(i))
    }

    var topK = new Array[Data](k)

    var HyperMap: List[Data] = List()
    var Hactors: List[HypActor] = List()
    println("pause test")
    mapOfNumCat.keys.foreach { i =>
      var actor = new HypActor
      actor.start
      Hactors = Hactors :+ actor
      actor ! (N, mapOfNumCat(i), i, k, topK)
    }

    while (HyperMap.size < mapOfNumCat.size){
      receive {
        case (x: BigDecimal, z: String) =>
          var inst = new Data()
          inst.setCategory(z)
          inst.setScore(x)
          HyperMap= HyperMap :+ inst
        case _ => println("wtf inside actor receiving ")
      }
    }

    //HyperMap= merge_sort(HyperMap)

    /*for (i <-0 until topK.length){
      println(topK(i).getScore())
    }*/

    HyperMap.foreach { i =>
      println("Key: "+ i.getCategory())
      println("Value: "+ mapOfNumCat(i.getCategory()) )
      println("hyperval: "+i.getScore())
    }

  }

}

