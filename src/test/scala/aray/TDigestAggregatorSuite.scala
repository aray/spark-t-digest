package aray

import scala.collection.JavaConversions._

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.{Column, SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.sql.functions.expr

/**
  * Created by andrew on 6/23/17.
  */
class TDigestAggregatorSuite extends FunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  private object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }
  import testImplicits._

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("version") {
    println(spark.version)
  }

  val percentiles = Seq(0.001d, 0.01d, 0.1d, 0.5d, 0.9d, 0.99d, 0.999d)
  val percentilesExpr = expr(percentiles.mkString("array(", "d, ", "d)")).expr

  test("t-digest correct for uniform random") {
    val df = spark.range(100000).selectExpr("rand() as r")
    val a = df.select(new Column(new TDigestAggregator(df("r").expr, percentilesExpr).toAggregateExpression()))
    a.show(false)
    val res = a.head().getList[Double](0)
    println(res)
    val eps = 0.005
    percentiles.zip(res).foreach { case (p, r) =>
      assert(math.abs(p - r) < eps, s"percentile $p result is $r differing by greater than $eps")
    }
  }

  test("t-digest correct for exp random") {
    val df = spark.range(100000).selectExpr("exp(rand()*50) as r")
    val a = df.select(new Column(new TDigestAggregator(df("r").expr, percentilesExpr).toAggregateExpression()))
    a.show(false)
    val res = a.head().getList[Double](0)
    println(res)
    val eps = 0.2
    percentiles.zip(res).foreach { case (p, r) =>
      val expected = math.exp(p * 50)
      assert(math.abs(math.log(expected) - math.log(r)) < eps, s"percentile $p result is $r, should be $expected differing log by greater than $eps")
    }
  }

  test("handles nulls, Nan, +/- inf") {
    val df = Seq(Some(1.0), None, Some(Double.MaxValue), Some(Double.MinValue), Some(Double.NaN)).toDS()
    val a = df.select(new Column(new TDigestAggregator(df("value").expr, percentilesExpr).toAggregateExpression()))
    a.show(false)

  }
}
