/**
 * Created by Grace on 2/28/2015.
 */

class Data {
  private var score = -1.0
  private var category = " "

  def getScore(): Double = {
    score
  }

  def getCategory() : String = {
    category
  }

  def setScore(x: Double) : Unit = {
    score = x;
  }

  def setCategory (x: String) : Unit = {
    category = x;
  }

  def print(): Unit = {
    println(score)
    println(category)
  }
}
