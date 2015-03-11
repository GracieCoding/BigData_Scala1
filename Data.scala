/**
 * Created by Gaberila & Grace.
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

class BData {
  private var score = BigDecimal(-1.0)
  private var category = " "

  def getScore(): BigDecimal = {
    score
  }

  def getCategory() : String = {
    category
  }

  def setScore(x: BigDecimal) : Unit = {
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