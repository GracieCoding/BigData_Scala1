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
  def isNewCategory(myList: List[Data], x: String, field: Field[Int]): Unit = {
    if (!myList.isEmpty){
      if (myList.head.getCategory() == x) {
        field.value = 0
      }
      isNewCategory(myList.tail, x, field)
    }
  }

  //Function to order the queue by
  def greaterThan(x: Data): Double = {
    x.getScore()
  }

  def isFull(counter: Int, k: Int) : Boolean = {
    if (counter >= k){
      true
    }
    else {
      false
    }
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
  }


  //actor creates slave actors that do the work, waits till they send back result to terminate
  class readMaster(filename:String, k:Int, parent:Actor) extends Actor{
    def act {
      var me =self
      var slave1 =new readActor
      var s2 = new readActor
      val Hrec= new recieveHActor(k)
      val Mrec= new recieveMActor
      var count= 0
      var H, M =true;
      while(H || M){
        receive {
          case "start" =>
            slave1.start()
            s2.start()
            Hrec.start()
            Mrec.start()
            for(line <- Source.fromFile(filename).getLines()){
              if(count%2==0)
                slave1 ! (line,Hrec,Mrec)
              else
                s2 ! (line,Hrec,Mrec)
              count+=1
            }
            self ! "Hwait"
            self ! "Mwait"
          case (x:PriorityQueue[(Data)]) =>
            parent ! x
            H=false
            Hrec ! "exit"
          case (x:Map[String,Int]) =>
            parent ! x
            M=false
            Mrec ! "exit"
          case "Hwait" =>
            Hrec ! count
          case "Mwait" =>
            Mrec ! count
          case "Hfalse" =>
            self ! "Hwait"
          case "Mfalse" =>
            self ! "Mwait"
          case "Htrue" =>
            Hrec ! "done"
            slave1 ! "exit"
            s2 ! "exit"
          case "Mtrue" =>
            Mrec ! "done"
            parent ! count
          case _ => println("dunno what happend master read actor")
        }
      }
    }
  }

  //Priorty queue listener, reports back to master when "done" ie local size = master count
  class recieveHActor(k:Int) extends Actor {
    def act {
      var me =self
      var myHeap = new PriorityQueue[(Data)]()(Ordering.by(greaterThan))
      var size =0
      while(true){
        receive {
          case (x:Data) =>
            if (isFull(size, k)){
              val it = Iterator(myHeap)
              myHeap = myHeap.filter(it => it != myHeap.min(Ordering.by(greaterThan)))
              myHeap += x
              size += 1
            }
            else {
              myHeap += x
              size += 1
            }
          case (x:Int) =>
            if(x==size)
              sender ! "Htrue"
            else
              sender ! "Hfalse"
          case "done" =>
            sender ! myHeap
          case "exit" => exit()
        }
      }
    }
  }

  //Map reciever Actor, reports back to master when "done" as soon as Hactor is done
  class recieveMActor extends Actor {
    var myMap: Map[String, Int] = Map()
    def act {
      while (true) {
        receive {
          case (x:Data) =>
            if (!myMap.contains(x.getCategory()))
              myMap += (x.getCategory() -> 1)
            else
              myMap.update(x.getCategory(), myMap(x.getCategory())+1)
          case (x:Int) =>
            var result: Int =0
            myMap.keys.foreach {i =>
              result+=myMap(i)}
            if(x==result)
              sender ! "Mtrue"
            else
              sender ! "Mfalse"
          case "done" => sender ! myMap
          case "exit" => exit()
        }
      }
    }
  }

  //processes line from master and sends it to the map and priority queue recievers
  class readActor extends Actor {
    var count=0
    var me = self
    def act {
      while(true){
        receive {
          case (line:String, h:Actor, m:Actor ) =>
            var dataInst = new Data()
            var data = line.split(" ")(0)
            dataInst.setScore(data.toDouble)
            data = line.split(" ")(1)
            dataInst.setCategory(data)
            count+=1
            h ! dataInst
            m ! dataInst
          case (line:Int) =>
            if (line/2 == count)
              sender ! true
            else
              sender ! false
          case "exit" => exit()
          case _ => println("dunno what happend read actor")

        }
      }
    }
  }


  def main(args: Array[String]) : Unit = {

    val me= self
    val fileName = args(0)
    var data = " "
    var dataList: List[Data] = List()
    var categoryNum = new Field(1)
    //pop size
    var N = 0
    val k= args(1).toInt


    var Mactor= new readMaster(fileName,k,self)
    Mactor.start()
    Mactor ! "start"

    var h, m= true
    var mapOfNumCat: Map[String, Int] = Map()
    var queue = new PriorityQueue[(Data)]()(Ordering.by(greaterThan))

    while(h || m)
    receive {
      case (x:Int) =>
        //popsize update
        N=x
      case (x:PriorityQueue[(Data)]) =>
        queue=x
        h=false
      case (y:Map[String, Int]) =>
        mapOfNumCat=y
        m=false
    }



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

    while ( !queue.isEmpty ){
      println(queue.dequeue().getScore())
    }
    println("hypermap size:"+HyperMap.size)
    HyperMap.foreach { i =>
      println("Key: "+ i.getCategory())
      println("Value: "+ mapOfNumCat(i.getCategory()) )
      println("hyperval: "+i.getScore())
    }

  }

}

