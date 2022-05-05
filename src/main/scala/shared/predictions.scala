package shared

import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

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

    def loadSpark(sc : SparkContext,  path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
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
    
    def scale(r_ui: Double, average_r_user: Double): Double = {
        var ret: Double = 1; // Default value
        if (r_ui > average_r_user) {
            ret = 5 - average_r_user
        }
        else if (r_ui < average_r_user) {
            ret = average_r_user - 1
        }
        return ret
    }

    def normalizedDeviation(rating: Double, averageR:Double): Double = {
        return (rating - averageR)/scale(rating,averageR)
    }

    def predictRating(averageUser: Double, averageDevItem: Double, usersInData: Boolean): Double = {    
        if (usersInData == false) {
            return averageUser // = globalAverage
        }
        else {
            return averageUser + averageDevItem*scale(averageUser + averageDevItem, averageUser) 
        }
    }

  	def globalAverage(s: CSCMatrix[Double]): Double = {
		return sum(s) / s.findAll(rating => rating != 0.0).length
	}

	def averageRatingUsers(s: CSCMatrix[Double]): DenseVector[Double] = {
		val data = s.toDense(*,::) // We work on each line
		val countNonZeros: DenseVector[Double] = data.map(o => o.findAll(rating => rating != 0.0).length.toDouble)
        
        val globalMean: Double = globalAverage(s); // Default value

        return (sum(data):/countNonZeros).map(o => if (o.isNaN() || o.isInfinity) globalMean else o)
	}

	def averageDeviationItems(s: CSCMatrix[Double], averageUsers: DenseVector[Double]): CSCMatrix[Double] = {
        val devRatingItemsPerUser_builder = new CSCMatrix.Builder[Double](rows=s.rows, cols=s.cols);
        for (((user,item),value) <- s.activeIterator) {
            devRatingItemsPerUser_builder.add(user, item, normalizedDeviation(value, averageUsers(user)))
        }
        return devRatingItemsPerUser_builder.result()
    }

    def cosineSimilarity(devRatingItemsPerUser: CSCMatrix[Double], k: Int): CSCMatrix[Double] = {
        val normsUsers: DenseVector[Double] = sum(devRatingItemsPerUser.mapActiveValues(o => o*o).toDense(*,::)).map(o => scala.math.sqrt(o))
        val halfSuv_builder = new CSCMatrix.Builder[Double](rows=devRatingItemsPerUser.rows, cols=devRatingItemsPerUser.cols);
        for (((user, item), value) <- devRatingItemsPerUser.activeIterator) {
            if (normsUsers(user) != 0.0) {
                halfSuv_builder.add(user, item, value/normsUsers(user))
            }
        }
        val halfSuv: DenseMatrix[Double] = halfSuv_builder.result().toDense
        val suvPerUserDense: DenseMatrix[Double] = (halfSuv * halfSuv.t)

        val suvPerUser_builder = new CSCMatrix.Builder[Double](rows=suvPerUserDense.rows, cols=suvPerUserDense.cols);
        for (u <- 0 until suvPerUserDense.rows) {
            val kNN_user: IndexedSeq[Int] = argtopk(suvPerUserDense(u, ::).t, k+1); // We'll drop the autosimilarity after
            for (v <- kNN_user) {
                suvPerUser_builder.add(u, v, suvPerUserDense(u,v)) // keep value only for kNN of user u
            }
        }
        return suvPerUser_builder.result()
    }

    def addAutoSimilarityZero(suvPerUser: CSCMatrix[Double]): CSCMatrix[Double] = {
        val suv: CSCMatrix[Double] = suvPerUser.copy
        for (u <- 0 until suv.rows) {
            suv.update(u, u, 0.0)
        }
        return suv
    }

    def ifZeroBecomeOne(x: Double): Double = {
        if (x == 0.0) {
            return 1.0
        }
        else {
            return x
        }
    }

    def indicator(x: Double): Double = {
        if (x == 0.0) {
            return 0.0
        }
        else {
            return 1.0
        }
    }

    def averageDeviationItemsCosine(s: CSCMatrix[Double], devRatingItemsPerUser: CSCMatrix[Double], suvPerUser: CSCMatrix[Double]): DenseMatrix[Double] = {
        val nonZerosIndicator: DenseMatrix[Double] = s.mapActiveValues(o => indicator(o)).toDense
        val averageDevItemsCos: DenseMatrix[Double] = (suvPerUser.toDense * devRatingItemsPerUser.toDense) /:/ ((suvPerUser.mapActiveValues(o => abs(o)).toDense * nonZerosIndicator).map(o => ifZeroBecomeOne(o)))
        return averageDevItemsCos
    }

    def kNN_builder(s: CSCMatrix[Double], k: Int): (DenseMatrix[Double], CSCMatrix[Double]) = {
        val averageUsers: DenseVector[Double] = averageRatingUsers(s)
        val devRatingItemsPerUser: CSCMatrix[Double] = averageDeviationItems(s, averageUsers) 
        val suvPerUser: CSCMatrix[Double] = cosineSimilarity(devRatingItemsPerUser, k)
        val averageDevItemsCos: DenseMatrix[Double] = averageDeviationItemsCosine(s, devRatingItemsPerUser, suvPerUser)

        val usersSet: DenseVector[Boolean] = s.toDense(*,::).map(o => any(o))

        val kNN_model_builder = new CSCMatrix.Builder[Double](rows=s.rows, cols=s.cols);
        for (user <- 0 until s.rows) {
            for (item <- 0 until s.cols) {
                kNN_model_builder.add(user, item, predictRating(averageUsers(user), averageDevItemsCos(user, item), usersSet(user)))
            }
        }
        return (kNN_model_builder.result().toDense, suvPerUser)
    }

    def computeMAE(s_test: CSCMatrix[Double], kNN_model: DenseMatrix[Double]): Double = {
        var kNN_MAE = 0.0;
        for (((user, item), value) <- s_test.activeIterator) {
            kNN_MAE += abs(value - kNN_model(user, item))
        }
        return kNN_MAE/s_test.activeSize.toDouble
    }

	///////////////////////// EK //////////////////////////////

    def kNN_builder_parallel(s: CSCMatrix[Double], k: Int, sc: SparkContext): (DenseMatrix[Double], CSCMatrix[Double]) = {
        val averageUsers: DenseVector[Double] = averageRatingUsers(s)
        val devRatingItemsPerUser: CSCMatrix[Double] = averageDeviationItems(s, averageUsers)
        val suvPerUser: CSCMatrix[Double] = cosineSimilarityParallel(devRatingItemsPerUser, k, sc) // Everything else could be parallelize but it's not what expected in the pdf
        val averageDevItemsCos: DenseMatrix[Double] = averageDeviationItemsCosine(s, devRatingItemsPerUser, suvPerUser)

        val usersSet: DenseVector[Boolean] = s.toDense(*,::).map(o => any(o))

        val kNN_model_builder = new CSCMatrix.Builder[Double](rows=s.rows, cols=s.cols);
        for (user <- 0 until s.rows) {
            for (item <- 0 until s.cols) {
                kNN_model_builder.add(user, item, predictRating(averageUsers(user), averageDevItemsCos(user, item), usersSet(user)))
            }
        }
        return (kNN_model_builder.result().toDense, suvPerUser) 
    }

    def topk(u: Int, br: Broadcast[DenseMatrix[Double]], k: Broadcast[Int]): IndexedSeq[((Int, Int), Double)] = {
        val suv_u: DenseVector[Double] = br.value * br.value.t(::, u)
        return argtopk(suv_u, k.value + 1).map(v => ((u, v), suv_u(v)))
    }

    def cosineSimilarityParallel(devRatingItemsPerUser: CSCMatrix[Double], k: Int, sc: SparkContext): CSCMatrix[Double] = {
        val normsUsers: DenseVector[Double] = sum(devRatingItemsPerUser.mapActiveValues(o => o*o).toDense(*,::)).map(o => scala.math.sqrt(o))
        val halfSuv_builder = new CSCMatrix.Builder[Double](rows=devRatingItemsPerUser.rows, cols=devRatingItemsPerUser.cols);
        for (((user, item), value) <- devRatingItemsPerUser.activeIterator) {
            if (normsUsers(user) != 0.0) {
                halfSuv_builder.add(user, item, value/normsUsers(user))
            }
        }
        val halfSuv: DenseMatrix[Double] = halfSuv_builder.result().toDense
        val nb_users: Int = halfSuv.rows

        val br: Broadcast[DenseMatrix[Double]] = sc.broadcast(halfSuv)
        val br_k: Broadcast[Int] = sc.broadcast(k)

        val topks: Array[IndexedSeq[((Int, Int), Double)]] = sc.parallelize(0 until nb_users).map(u => topk(u, br, br_k)).collect()

        val suvPerUser_builder = new CSCMatrix.Builder[Double](rows=nb_users, cols=nb_users)
        for (topks_node <- topks) {
            for (((u, v), value) <- topks_node) {
                suvPerUser_builder.add(u, v, value)
            }
        }
        return suvPerUser_builder.result()
    }

    ///////////////////////// AK //////////////////////////////

    def kNN_builder_parallel_approx(s: CSCMatrix[Double], k: Int, sc: SparkContext, users: Seq[Set[Int]]): (DenseMatrix[Double], CSCMatrix[Double]) = {
        val averageUsers: DenseVector[Double] = averageRatingUsers(s)
        val devRatingItemsPerUser: CSCMatrix[Double] = averageDeviationItems(s, averageUsers) 
        val suvPerUser: CSCMatrix[Double] = cosineSimilarityParallelApprox(devRatingItemsPerUser, k, sc, users)
        val averageDevItemsCos: DenseMatrix[Double] = averageDeviationItemsCosine(s, devRatingItemsPerUser, suvPerUser)

        val usersSet: DenseVector[Boolean] = s.toDense(*,::).map(o => any(o))

        val kNN_model_builder = new CSCMatrix.Builder[Double](rows=s.rows, cols=s.cols);
        for (user <- 0 until s.rows) {
            for (item <- 0 until s.cols) {
                kNN_model_builder.add(user, item, predictRating(averageUsers(user), averageDevItemsCos(user, item), usersSet(user)))
            }
        }
        return (kNN_model_builder.result().toDense, suvPerUser)
    }

    def topkApprox(setU: Seq[Int], br: Broadcast[DenseMatrix[Double]], k: Broadcast[Int]): IndexedSeq[(Int, Int, Double)] = {
        val subMatrix_devRatingItemsPerUser: DenseMatrix[Double] = br.value(setU, ::).toDenseMatrix // toDenseMatrix is useless if never only one line in setU
        val subMatrix_suvPerUserDense: DenseMatrix[Double] = subMatrix_devRatingItemsPerUser * subMatrix_devRatingItemsPerUser.t
        
        val nb_users: Int = subMatrix_suvPerUserDense.rows

        val kNN_user: IndexedSeq[(Int, Int, Double)] = (0 until nb_users)
            .map(u => argtopk(subMatrix_suvPerUserDense(u, ::).t, scala.math.min(k.value+1,nb_users)) // We'll drop the autosimilarity after
            .map(v => (setU(u), setU(v), subMatrix_suvPerUserDense(u, v))))
            .flatten
        return kNN_user
    }

    def cosineSimilarityParallelApprox(devRatingItemsPerUser: CSCMatrix[Double], k: Int, sc: SparkContext, users: Seq[Set[Int]]): CSCMatrix[Double] = {
        val normsUsers: DenseVector[Double] = sum(devRatingItemsPerUser.mapActiveValues(o => o*o).toDense(*,::)).map(o => scala.math.sqrt(o))
        val halfSuv_builder = new CSCMatrix.Builder[Double](rows=devRatingItemsPerUser.rows, cols=devRatingItemsPerUser.cols);
        for (((user, item), value) <- devRatingItemsPerUser.activeIterator) {
            if (normsUsers(user) != 0.0) {
                halfSuv_builder.add(user, item, value/normsUsers(user))
            }
        }
        val halfSuv: DenseMatrix[Double] = halfSuv_builder.result().toDense
        val nb_users: Int = halfSuv.rows

        val br: Broadcast[DenseMatrix[Double]] = sc.broadcast(halfSuv)
        val br_k: Broadcast[Int] = sc.broadcast(k)

        val topks: Array[IndexedSeq[(Int, Int, Double)]] = sc.parallelize(users).map(setU => topkApprox(setU.toSeq, br, br_k)).collect()

        val topks_choosen: Map[Int, Array[(Int, Int, Double)]] = topks.flatten.groupBy(o => o._1)
            .map(u => (u._1, u._2.toSet.toArray.sortWith(_._3 > _._3).slice(0, k+1)))

        val suvPerUser_builder = new CSCMatrix.Builder[Double](rows=nb_users, cols=nb_users)
        for (suv_u <- topks_choosen.valuesIterator) {
            for ((u, v, value) <- suv_u) {
                suvPerUser_builder.add(u, v, value)
            }
        }
        return suvPerUser_builder.result()
    }    

}