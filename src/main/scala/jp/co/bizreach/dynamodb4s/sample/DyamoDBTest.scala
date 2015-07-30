package jp.co.bizreach.dynamodb4s.sample

import jp.co.bizreach.dynamodb4s._
import awscala.dynamodbv2.{DynamoDBCondition, DynamoDB}

object Members extends DynamoTable {
  protected val table = "members"
  val id      = DynamoHashKey[Int]("id")
  val country = DynamoRangeKey[String]("country")
  val name    = DynamoAttribute[String]("name")
  val age     = DynamoAttribute[Int]("age")
  val company = DynamoAttribute[String]("company")

}

class Member(
  val id: Int,
  val country: String,
  val name: String,
  val age: Int,
  val company: Option[String]
)

object DynamoDBTest extends App {

  implicit val db = DynamoDB.local()

  //  Members.put(Member(1, "Japan", "Takezoe", 32, "BizR"))

  Members.putAttributes(1, "Japan"){ t =>
    t.name -> "xxx" :: Nil
  }

  val list = Members.query.
    keyConditions { t =>
      t.id -> DynamoDBCondition.eq(1) :: Nil
    }
    .as[Member]

  list.foreach { x =>
    println(x.id)
    println(x.country)
    println(x.name)
    println(x.age)
    println(x.company)
  }

  val names = Members.query
    .attribute(_.id)
    .attribute(_.country)
    .attribute(_.name)
    .attribute(_.company)
    .keyCondition(_.id -> DynamoDBCondition.eq(1))
    .limit(100000)
    .map { (t, x) =>
      (x.get(t.id), x.get(t.country), x.get(t.name), x.get(t.company))
    }

  println(names)
}
