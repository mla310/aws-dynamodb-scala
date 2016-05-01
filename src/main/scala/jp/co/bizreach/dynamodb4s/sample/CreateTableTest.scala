package jp.co.bizreach.dynamodb4s.sample

/**
 * This is a test code of AWScala DynamoDB interface.
 */
object CreateTableTest extends App {
  import awscala._
  import dynamodbv2._

  implicit val dynamoDB = dynamodbv2.DynamoDB.local()

  if(dynamoDB.tableNames.contains("members")){
    dynamoDB.deleteTable("members")
  }

  val tableMeta: TableMeta = dynamoDB.createTable(
    name    = "members",
    hashPK  = "country" -> AttributeType.String,
    rangePK = "id"      -> AttributeType.Number,
    otherAttributes = Seq("company" -> AttributeType.String),
    indexes = Seq(LocalSecondaryIndex(
      name       = "companyIndex",
      keySchema  = Seq(KeySchema("country", KeyType.Hash), KeySchema("company", KeyType.Range)),
      projection = Projection(ProjectionType.Include, Seq("id", "country", "name", "age", "company"))
    ))
  )

  val table: Table = dynamoDB.table("members").get

  try {
    table.put("Japan", 1, "name" -> "Alice", "age" -> 23, "company" -> "Google")
    table.put("U.S.",  2, "name" -> "Bob",   "age" -> 36, "company" -> "Google")
    table.put("Japan", 3, "name" -> "Chris", "age" -> 29, "company" -> "Amazon")

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
