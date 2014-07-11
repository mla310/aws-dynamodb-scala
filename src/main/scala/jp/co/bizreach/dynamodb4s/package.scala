package jp.co.bizreach

package object dynamodb4s {

  trait DynamoDataType[T] {
    def convert(value: Any): T
  }

  implicit val IntDynamoType = new DynamoDataType[Int]{
    def convert(value: Any): Int = value.asInstanceOf[String].toInt
  }
  implicit val LongDynamoType = new DynamoDataType[Long]{
    def convert(value: Any): Long = value.asInstanceOf[String].toLong
  }
  implicit val StringDynamoType = new DynamoDataType[String]{
    def convert(value: Any): String = value.asInstanceOf[String]
  }
  implicit val IntListDynamoType = new DynamoDataType[List[Int]]{
    def convert(value: Any): List[Int] = value.asInstanceOf[List[String]].map(_.toInt)
  }
  implicit val LongListDynamoType = new DynamoDataType[List[Long]]{
    def convert(value: Any): List[Long] = value.asInstanceOf[List[String]].map(_.toLong)
  }
  implicit val StringListDynamoType = new DynamoDataType[List[String]]{
    def convert(value: Any): List[String] = value.asInstanceOf[List[String]]
  }

  trait DynamoKey { val name: String }
  trait DynamoProperty[T] {
    val name: String
    def convert(value: Any): T
  }
  case class DynamoHashKey[T](name: String)(implicit t: DynamoDataType[T]) extends DynamoKey with DynamoProperty[T] {
    def convert(value: Any): T = t.convert(value)
  }
  case class DynamoRangeKey[T](name: String)(implicit t: DynamoDataType[T]) extends DynamoKey with DynamoProperty[T] {
    def convert(value: Any): T = t.convert(value)
  }
  case class DynamoAttribute[T](name: String)(implicit t: DynamoDataType[T]) extends DynamoProperty[T] {
    def convert(value: Any): T = t.convert(value)
  }

}
