package bdm

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.OneHotEncoder

case class Sample(neigh: String, price: Double, rfd: Double)

object Train extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[4]")
      .appName("myApp")
      .getOrCreate()

  try {

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = spark.read
      .option("header", true)
      .csv("datasets/p3_integrated.csv")

    val data = df.map { row =>
      val neigh = row.getAs[String]("neigh_id")
      val price = row.getAs[String]("price").toDouble
      val rfd = row.getAs[String]("RFD").toDouble
      Sample(neigh, price, rfd)
    }

    val Array(training, test) = data.randomSplit(Array(0.7, 0.3))

    val assembler1 = new VectorAssembler()
      .setInputCols(Array("price"))
      .setOutputCol("numericals")

    val scaler = new StandardScaler()
      .setInputCol("numericals")
      .setOutputCol("numericals_std")
      .setWithStd(true)
      .setWithMean(true)

    val indexer = new StringIndexer()
      .setInputCol("neigh")
      .setOutputCol("neigh_cat")
      .setHandleInvalid("keep")

    // You can't apply one hot encoding without StringIndexer first.
    val oneHot = new OneHotEncoder()
      .setInputCol("neigh_cat")
      .setOutputCol("categoricals")

    val assembler2 = new VectorAssembler()
      .setInputCols(Array("numericals_std", "categoricals"))
      .setOutputCol("features")

    val lr = new LinearRegression()
      .setLabelCol("rfd") // target
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setRegParam(0.1)

    val pipeline = new Pipeline()
      .setStages(Array(assembler1, indexer, oneHot, scaler, assembler2, lr))

    val model = pipeline.fit(training)

    model.write.overwrite().save("datasets/lr-model")

    val predictions = model.transform(test)

    predictions.select("neigh", "price", "prediction").show(5)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("rfd")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println(s"RMSE: $rmse")
  } catch {
    case e: Throwable =>
      print(e.getMessage())
  } finally {
    spark.stop()
  }
}
