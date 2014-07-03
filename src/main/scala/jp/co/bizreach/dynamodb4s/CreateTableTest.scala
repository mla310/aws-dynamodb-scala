package jp.co.bizreach.dynamodb4s

import scala.annotation.StaticAnnotation

/**
 * This is a test code of AWSScala DynamoDB interface.
 */
object CreateTableTest extends App {
  import awscala._
  import dynamodbv2._

  implicit val dynamoDB = dynamodbv2.DynamoDB.local()

  val tableMeta: TableMeta = dynamoDB.createTable(
    name    = "members",
    hashPK  = "id"      -> AttributeType.Number,
    rangePK = "country" -> AttributeType.String,
    otherAttributes = Seq("company" -> AttributeType.String),
    indexes = Seq(LocalSecondaryIndex(
      name       = "companyIndex",
      keySchema  = Seq(KeySchema("id", KeyType.Hash), KeySchema("company", KeyType.Range)),
      projection = Projection(ProjectionType.Include, Seq("company"))
    ))
  )

  val table: Table = dynamoDB.table("members").get

  try {
    table.put(1, "Japan", "name" -> "Alice", "age" -> 23, "company" -> "Google")
    table.put(2, "U.S.",  "name" -> "Bob",   "age" -> 36, "company" -> "Google")
    table.put(3, "Japan", "name" -> "Chris", "age" -> 29, "company" -> "Amazon")

//    table.deleteItem(1, "Japan")
//    //dynamoDB.deleteItem(table, 1, "Japan")
//    table.putAttributes(1, "Japan", Seq("name" -> "xxxx"))
//
//    //table.deleteItem(1, "Japan")
//
//    val googlers: Seq[Item] = table.scan(Seq("Country" -> Condition.eq("Japan")))
//    googlers.foreach { x =>
//      println(x.attributes)
//    }
  } finally {
//    table.destroy()
  }
}

//@DynamoTable(name = "Members")
//case class Member(@HashPk Id: Int, @RangePk Country: String, Name: String, Age: Int, Company: String)
