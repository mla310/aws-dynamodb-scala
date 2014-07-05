package jp.co.bizreach.dynamodb4s.sample

import jp.co.bizreach.dynamodb4s._
import awscala.dynamodbv2.{Condition, DynamoDB}

object Members extends DynamoTable {
  protected val table = "members"
  val name    = DynamoAttribute("name")
  val age     = DynamoAttribute("age")
  val company = DynamoAttribute("company")
}

class Member(
  @hashPk val id: Int,
  @rangePk val country: String,
  val name: String,
  val age: Int,
  val company: Option[String]
)

object DynamoDBTest extends App {

  implicit val db = DynamoDB.local()

  //  Members.put(Member(1, "Japan", "Takezoe", 32, "BizR"))

  Members.putAttributes(1, "Japan"){ t =>
    Seq(t.name -> "xxx")
  }

  val list = Members.query[Member](keyConditions = Seq("id" -> Condition.eq(1)))
  list.foreach { x =>
    println(x.id)
    println(x.country)
    println(x.name)
    println(x.age)
    println(x.company)
  }

  //  Members.putAttributes(1, "Japan"){ t =>
  //    Seq(t.company -> "BizReach")
  //  }

  //  Members.putWithCondition(Member(1, "Japan", "Chris", 32, "Google")){ t =>
  //    Seq(t.company -> Condition.eq(0))
  //  }


}