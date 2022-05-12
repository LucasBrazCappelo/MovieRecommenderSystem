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

    val priceM7 = 38600.0
    val rentingDay = 20.40
    val vCPUs = 1.14e-6
    val nbrVCPU = 1.0 
    val cGBs = 1.6e-7
    val nbGB = 32.0
    val nbRPis = 4.0
    val energyCost = 0.25
    val priceRPI4 = 108.48
    val ramICCM7 = 24.0 * 64.0
    val ramRPIs = 8.0
    val nbrCICCM7 = 2.0 * 14.0
    val throughPutICCM7 = 2.6
    val nbrCRPIs = 4.0
    val throughPutRPIs = 1.5

    val e1 :Double = (priceM7 / rentingDay).ceil.toDouble
    val e21:Double = (vCPUs * nbrVCPU + cGBs * nbGB)*3600*24
    val e22:Double= nbRPis * 3 * energyCost *24/1000
    val e23:Double =nbRPis * 4 * energyCost *24/1000
    val e24:Double= (priceRPI4 / e22).ceil
    val e25:Double = (priceRPI4 / e23).ceil
    val e31:Double = (priceM7 / priceRPI4).floor
    val e32:Double = ramICCM7 / (e31 * ramRPIs)
    val e33:Double = (nbrCICCM7 * throughPutICCM7) / (e31 * nbrCRPIs * throughPutRPIs)
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
