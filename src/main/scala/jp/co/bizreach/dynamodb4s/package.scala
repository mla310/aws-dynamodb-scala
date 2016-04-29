package jp.co.bizreach

package object dynamodb4s {

  trait DynamoDataType[T] {
    def convert(value: Any): T
  }

  implicit val IntDynamoType = new DynamoDataType[Int]{
    def convert(value: Any): Int = value match {
      case Some(x: Int)  => x
      case Some(x: Long) => x.toInt
      case Some(x: Any)  => x.toString.toInt
      case None          => 0
      case x: Int        => x
      case x: Long       => x.toInt
      case x             => x.toString.toInt
    }
  }
  implicit val LongDynamoType = new DynamoDataType[Long]{
    def convert(value: Any): Long = value match {
      case Some(x: Int)  => x.toLong
      case Some(x: Long) => x
      case Some(x)       => x.toString.toLong
      case None          => 0
      case x: Int        => x.toLong
      case x: Long       => x
      case x             => x.toString.toLong
    }
  }
  implicit val StringDynamoType = new DynamoDataType[String]{
    def convert(value: Any): String = value match {
      case Some(x) => x.toString
      case None    => null
      case x       => x.toString
    }
  }
  implicit val IntListDynamoType = new DynamoDataType[List[Int]]{
    def convert(value: Any): List[Int] = value.asInstanceOf[List[Any]].map(IntDynamoType.convert)
  }
  implicit val LongListDynamoType = new DynamoDataType[List[Long]]{
    def convert(value: Any): List[Long] = value.asInstanceOf[List[Any]].map(LongDynamoType.convert)
  }
  implicit val StringListDynamoType = new DynamoDataType[List[String]]{
    def convert(value: Any): List[String] = value.asInstanceOf[List[Any]].map(StringDynamoType.convert)
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
