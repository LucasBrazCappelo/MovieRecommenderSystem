import org.rogach.scallop._
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import ujson._

package economics {

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}

object Economics {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)
    val e1 :Double = (38600.0/20.40).ceil.toDouble
    val e21:Double = (1.14e-6+1.6e-7*32) *3600*24
    val e22:Double=4*3*0.25*24/1000
    val e23:Double =4*4*0.25*24/1000
    val e24:Double= (108.48/e22).ceil
    val e25:Double = (108.48/e23).ceil
    val e31:Double = (38600/108.48).floor
    val e32:Double = 8/(e31*24*64)
    val e33:Double = (2*14*2.6) / (e31*4*1.5)
    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json") =
      Some(new java.io.PrintWriter(location)).foreach{
        f => try{
          f.write(content)
        } finally{ f.close }
    }
    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {


        val answers = ujson.Obj(
          "E.1" -> ujson.Obj(
            "MinRentingDays" -> ujson.Num(e1) // Datatype of answer: Double
          ),
          "E.2" -> ujson.Obj(
            "ContainerDailyCost" -> ujson.Num(e21),
            "4RPisDailyCostIdle" -> ujson.Num(e22),
            "4RPisDailyCostComputing" -> ujson.Num(e23),
            "MinRentingDaysIdleRPiPower" -> ujson.Num(e24),
            "MinRentingDaysComputingRPiPower" -> ujson.Num(e25) 
          ),
          "E.3" -> ujson.Obj(
            "NbRPisEqBuyingICCM7" -> ujson.Num(e31),
            "RatioRAMRPisVsICCM7" -> ujson.Num(e32),
            "RatioComputeRPisVsICCM7" -> ujson.Num(e33)
          )
        )

        val json = write(answers, 4)
        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}

}
