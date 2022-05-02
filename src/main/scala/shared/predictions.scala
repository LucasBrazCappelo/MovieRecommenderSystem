package shared

import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext

import scala.util.control.Breaks._

package object predictions
{
    // ------------------------ For template
    case class Rating(user: Int, item: Int, rating: Double)

    def timingInMs(f : ()=>Double ) : (Double, Double) = {
        val start = System.nanoTime() 
        val output = f()
        val end = System.nanoTime()
        return (output, (end-start)/1000000.0)
    }

    def toInt(s: String): Option[Int] = {
        try {
          Some(s.toInt)
        } catch {
          case e: Exception => None
        }
    }

    def mean(s :Seq[Double]): Double =  if (s.size > 0) s.reduce(_+_) / s.length else 0.0

    def std(s :Seq[Double]): Double = {
        if (s.size == 0) 0.0
        else { 
            val m = mean(s)
            scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble) 
        }
    }


    def load(path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
        val file = Source.fromFile(path)
        val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies) 
        for (line <- file.getLines) {
            val cols = line.split(sep).map(_.trim)
            toInt(cols(0)) match {
                case Some(_) => builder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
                case None => None
            }
        }
        file.close
        builder.result()
    }

    def loadSpark(sc : org.apache.spark.SparkContext,  path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
        val file = sc.textFile(path)
        val ratings = file
            .map(l => {
                val cols = l.split(sep).map(_.trim)
                toInt(cols(0)) match {
                    case Some(_) => Some(((cols(0).toInt-1, cols(1).toInt-1), cols(2).toDouble))
                    case None => None
                }
            })
            .filter({ case Some(_) => true
                      case None => false })
            .map({ case Some(x) => x
                   case None => ((-1, -1), -1) }).collect()

        val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies)
        for ((k,v) <- ratings) {
            v match {
                case d: Double => {
                    val u = k._1
                    val i = k._2
                    builder.add(u, i, d)
                }
            }
        }
        return builder.result
    }

    def partitionUsers (nbUsers : Int, nbPartitions : Int, replication : Int) : Seq[Set[Int]] = {
        val r = new scala.util.Random(1337)
        val bins : Map[Int, collection.mutable.ListBuffer[Int]] = (0 to (nbPartitions-1))
            .map(p => (p -> collection.mutable.ListBuffer[Int]())).toMap
        (0 to (nbUsers-1)).foreach(u => {
            val assignedBins = r.shuffle(0 to (nbPartitions-1)).take(replication)
            for (b <- assignedBins) {
                bins(b) += u
            }
        })
        bins.values.toSeq.map(_.toSet)
    }

	///////////////////////// BR //////////////////////////////

	def scale(r_ui: Double, average_r_user: Double):Double = {
        var ret: Double = 1; // Default value
        if (r_ui > average_r_user) {
            ret = 5 - average_r_user
        }
        else if (r_ui < average_r_user) {
            ret = average_r_user - 1
        }
        return ret
    }

    def normalizedDeviation(rating: Double, averageR:Double):Double = {
        return (rating - averageR)/scale(rating,averageR)
    }

    def predictRating(averageUser: Double, averageDevItem: Double, usersInData: Boolean):Double = {    
        if (usersInData == false) {
            return averageUser // = globalAverage
        }
        else {
            return averageUser + averageDevItem*scale(averageUser+averageDevItem, averageUser) 
        }
    }

  	def globalAverage(s: CSCMatrix[Double]): Double = {
		return sum(s) / s.findAll(rating => rating != 0.0).size
	}

	def averageRatingUsers(s: CSCMatrix[Double]): DenseVector[Double] = {
		val data = s.toDense(*,::) // We work on each line
		val countNonZeros = data.map(_.foldLeft(0.0)((acc, num) => if (num != 0.0) acc + 1.0 else acc))
        
        val globalMean = globalAverage(s); // Default value

        return (sum(data):/countNonZeros).map(o => if (o.isNaN() || o.isInfinity) globalMean else o)
	}

	def averageDeviationItems(s: CSCMatrix[Double], averageUsers: DenseVector[Double]): DenseMatrix[Double] = {
        val ret_builder = new CSCMatrix.Builder[Double](rows=s.rows, cols=s.cols);
        for (((user,item),value) <- s.activeIterator) {
            ret_builder.add(user, item, normalizedDeviation(value, averageUsers(user)))
        }
        return ret_builder.result().toDense // devRatingItemsPerUser # TODO: Dommage que ce soit un DenseMatrix
    }

    def cosineSimilarity(devRatingItemsPerUser: DenseMatrix[Double]): DenseMatrix[Double] = {
        val normsUsers = sum(devRatingItemsPerUser*:*devRatingItemsPerUser, Axis._1).map(o => scala.math.sqrt(o))
        val ret_builder = new CSCMatrix.Builder[Double](rows=devRatingItemsPerUser.rows, cols=devRatingItemsPerUser.cols);
        for (user <- 0 until devRatingItemsPerUser.rows) {
            breakable {
                for (item <- 0 until devRatingItemsPerUser.cols) {
                    if (normsUsers(user) != 0.0) {
                        ret_builder.add(user, item, devRatingItemsPerUser(user,item)/normsUsers(user))
                    }
                    else {
                        break
                    }
                }
            }
        }
        val ret = ret_builder.result().toDense
        return ret * ret.t // suvPerUser
    }

    def keepKnnSuv(k: Int, suvPerUser: DenseMatrix[Double], firstInt:Int = 1): DenseMatrix[Double] = {
        for (x <- 0 until suvPerUser.rows) {
            val kNN_user = argtopk(suvPerUser(x, ::).t, k+1).toArray.slice(firstInt, k + 1); // 1 to k+1 because first one is the user itself
            for (y <- 0 until suvPerUser.cols) {
                if (!kNN_user.contains(y)) {
                    suvPerUser(x, y) = 0.0 // keep value only for kNN of user x
                }
            }
        }
        return suvPerUser
    }

    def similarityFromNothing(s: CSCMatrix[Double], k: Int): DenseMatrix[Double] = {
        val averageUsers = averageRatingUsers(s)
        val devRatingItemsPerUser = averageDeviationItems(s, averageUsers)
        val suvPerUser = cosineSimilarity(devRatingItemsPerUser)
        return keepKnnSuv(k, suvPerUser)
    }

    def averageDeviationItemsCosine(s: CSCMatrix[Double], devRatingItemsPerUser: DenseMatrix[Double], kNN_User_Similarity: DenseMatrix[Double]): DenseMatrix[Double] = {
        val nonZerosIndicator = s.toDense.map(o => if (o != 0.0) 1.0 else 0.0)
        val ret = (kNN_User_Similarity * devRatingItemsPerUser) /:/ ((kNN_User_Similarity.map(o => abs(o)) * nonZerosIndicator).map(o => if (o != 0.0) o else 1.0))
        return ret
    }

    def kNN_builder(s: CSCMatrix[Double], k: Int) : DenseMatrix[Double] = {
        val averageUsers = averageRatingUsers(s)
        val devRatingItemsPerUser = averageDeviationItems(s, averageUsers) // use normalizedDeviation : default value 0.0 if (u,i) not in s
        val suvPerUser = cosineSimilarity(devRatingItemsPerUser)
        val kNN_User_Similarity = keepKnnSuv(k, suvPerUser,0) // real suvPerUser
        val averageDevItemsCos = averageDeviationItemsCosine(s, devRatingItemsPerUser, kNN_User_Similarity)

        val usersSet = s.toDense(*,::).map(o => any(o))

        val ret_builder = new CSCMatrix.Builder[Double](rows=s.rows, cols=s.cols); // predictRatingCosineKNN Matrix builder
        for (user <- 0 until s.rows) {
            for (item <- 0 until s.cols) {
                ret_builder.add(user, item, predictRating(averageUsers(user), averageDevItemsCos(user, item), usersSet(user)))
            }
        }
        return ret_builder.result().toDense
    }

    def MAE(s_test: CSCMatrix[Double], kNN_model: DenseMatrix[Double]): Double = {
        val errors = (for (((user,item),value) <- s_test.activeIterator) yield abs(kNN_model(user, item) - value)).toList
        return mean(errors)
    }

	///////////////////////// EK //////////////////////////////

    

    ///////////////////////// AK //////////////////////////////



    ///////////////////////// E  //////////////////////////////



}