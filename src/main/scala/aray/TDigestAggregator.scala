package aray

import java.nio._

import com.tdunning.math.stats.MergingDigest

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/**
  * Created by andrew on 6/22/17.
  */
case class TDigestAggregator(
                              child: Expression,
                              percentageExpression: Expression,
                              accuracyExpression: Expression,
                              override val mutableAggBufferOffset: Int,
                              override val inputAggBufferOffset: Int
                            ) extends TypedImperativeAggregate[MergingDigest]{

  def this(child: Expression, percentageExpression: Expression, accuracyExpression: Expression) = {
    this(child, percentageExpression, accuracyExpression, 0, 0)
  }

  def this(child: Expression, percentageExpression: Expression) = {
    this(child, percentageExpression, Literal(1024))
  }

  // Mark as lazy so that accuracyExpression is not evaluated during tree transformation.
  private lazy val accuracy: Int = accuracyExpression.eval().asInstanceOf[Int]

  override def inputTypes: Seq[DataType] = Seq(DoubleType, ArrayType(DoubleType), IntegerType)

  // Mark as lazy so that percentageExpression is not evaluated during tree transformation.
  private lazy val percentages: Array[Double] = {
    Cast(percentageExpression,ArrayType(DoubleType)).eval() match {
      case arrayData: ArrayData =>
        arrayData.toDoubleArray()
      case other =>
        throw new IllegalArgumentException(s"Invalid data type ${percentageExpression.dataType} for parameter percentage")
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!percentageExpression.foldable || !accuracyExpression.foldable) {
      TypeCheckFailure(s"The accuracy or percentage provided must be a constant literal")
    } else if (accuracy <= 0) {
      TypeCheckFailure(
        s"The accuracy provided must be a positive integer literal (current value = $accuracy)")
    } else if (percentages.exists(percentage => percentage < 0.0D || percentage > 1.0D)) {
      TypeCheckFailure(
        s"All percentage values must be between 0.0 and 1.0 " +
          s"(current = ${percentages.mkString(", ")})")
    } else {
      TypeCheckSuccess
    }
  }

  override def createAggregationBuffer(): MergingDigest = new MergingDigest(accuracy)

  override def update(buffer: MergingDigest, input: InternalRow): Unit = {
    val value = child.eval(input)
    if (value != null && !value.asInstanceOf[Double].isNaN)
      buffer.add(value.asInstanceOf[Double])
  }

  override def merge(buffer: MergingDigest, input: MergingDigest): Unit = {
    buffer.add(input)
  }

  override def eval(buffer: MergingDigest): Any = ArrayData.toArrayData(percentages.map(buffer.quantile))

  override def serialize(buffer: MergingDigest): Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(buffer.byteSize())
    buffer.asSmallBytes(byteBuffer)
    byteBuffer.array()
  }

  override def deserialize(bytes: Array[Byte]): MergingDigest = MergingDigest.fromBytes(ByteBuffer.wrap(bytes))

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(DoubleType)

  override def children: Seq[Expression] = Seq(child, percentageExpression, accuracyExpression)
}
