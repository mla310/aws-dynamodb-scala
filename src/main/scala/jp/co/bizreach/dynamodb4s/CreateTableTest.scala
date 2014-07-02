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
    name    = "Members",
    hashPK  = "Id"      -> AttributeType.Number,
    rangePK = "Country" -> AttributeType.String,
    otherAttributes = Seq("Company" -> AttributeType.String),
    indexes = Seq(LocalSecondaryIndex(
      name       = "CompanyIndex",
      keySchema  = Seq(KeySchema("Id", KeyType.Hash), KeySchema("Company", KeyType.Range)),
      projection = Projection(ProjectionType.Include, Seq("Company"))
    ))
  )

  val table: Table = dynamoDB.table("Members").get

  try {
    table.put(1, "Japan", "Name" -> "Alice", "Age" -> 23, "Company" -> "Google")
    table.put(2, "U.S.",  "Name" -> "Bob",   "Age" -> 36, "Company" -> "Google")
    table.put(3, "Japan", "Name" -> "Chris", "Age" -> 29, "Company" -> "Amazon")

    table.deleteItem(1, "Japan")
    //dynamoDB.deleteItem(table, 1, "Japan")
    table.putAttributes(1, "Japan", Seq("Name" -> "xxxx"))

    //table.deleteItem(1, "Japan")

    val googlers: Seq[Item] = table.scan(Seq("Country" -> Condition.eq("Japan")))
    googlers.foreach { x =>
      println(x.attributes)
    }
  } finally {
    table.destroy()
  }
}

//@DynamoTable(name = "Members")
//case class Member(@HashPk Id: Int, @RangePk Country: String, Name: String, Age: Int, Company: String)
