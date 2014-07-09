package jp.co.bizreach

package object dynamodb4s {

  trait DynamoType[T]

  implicit val IntDynamoType = new DynamoType[Int]{}
  implicit val StringDynamoType = new DynamoType[String]{}

  trait DynamoKey { val name: String }
  case class DynamoHashKey[T](name: String)(implicit t: DynamoType[T]) extends DynamoKey
  case class DynamoRangeKey[T](name: String)(implicit t: DynamoType[T]) extends DynamoKey
  case class DynamoAttribute[T](name: String)(implicit t: DynamoType[T])

}
