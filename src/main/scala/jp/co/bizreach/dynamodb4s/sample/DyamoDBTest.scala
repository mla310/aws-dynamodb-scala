package jp.co.bizreach.dynamodb4s.sample

import jp.co.bizreach.dynamodb4s._
import awscala.dynamodbv2.{DynamoDBCondition, DynamoDB}

// TODO Is it possible to create table from this definition?
object Members extends DynamoTable {
  val table   = "members"
  val country = DynamoHashKey[String]("country")
  val id      = DynamoRangeKey[Int]("id")
  val name    = DynamoAttribute[String]("name")
  val age     = DynamoAttribute[Int]("age")
  val company = DynamoAttribute[String]("company")

  object companyIndex extends DynamoTable.SecondaryIndex {
    val index   = "companyIndex"
    val country = DynamoHashKey[String]("country")
    val company = DynamoRangeKey[String]("company")
  }
}

case class Member(
  country: String,
  id: Int,
  name: String,
  age: Int,
  company: Option[String]
)

object DynamoDBTest extends App {

  implicit val db = DynamoDB.local()

  // Insert
  Members.put(Member("Japan", 1, "Takezoe",   37, Some("BizReach")))
  Members.put(Member("Japan", 2, "Shimamoto", 33, Some("BizReach")))

  Members.query.keyConditions { t =>
    t.country -> DynamoDBCondition.eq("Japan") :: t.id -> DynamoDBCondition.eq(1) :: Nil
  }.firstOption[Member].foreach(println)

  // Update
  Members.putAttributes("Japan", 1){ t =>
    t.name -> "Naoki" :: Nil
  }

  Members.query.keyConditions { t =>
    t.country -> DynamoDBCondition.eq("Japan") :: t.id -> DynamoDBCondition.eq(1) :: Nil
  }.firstOption[Member].foreach(println)

//  Members.query.keyConditions { t =>
//    t.country -> DynamoDBCondition.eq("Japan") :: Nil
//  }.list[Member].foreach(println)

  // Query using secondary index
  println("-- Query using secondary index --")
  println(Members.query.secondaryIndexCondition(_.companyIndex){ t =>
    t.country -> DynamoDBCondition.eq("Japan") :: t.company -> DynamoDBCondition.eq("BizReach") :: Nil
  }.list[Member])

  // Scan
  println("-- Scan --")
  println(Members.scan.filterExpression("company = :company", "company" -> "BizReach").as[Member]{ x =>
    println(x)
  })


  val names = Members.query
    .attribute(_.id)
    .attribute(_.country)
    .attribute(_.name)
    .attribute(_.company)
    .keyCondition(_.country -> DynamoDBCondition.eq("Japan"))
    .limit(100000)
    .map { (t, x) =>
      (x.get(t.id), x.get(t.country), x.get(t.name), x.get(t.company))
    }

  println(names)
}
