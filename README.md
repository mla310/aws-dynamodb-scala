aws-dynamodb-scala
==========

Scala client for Amazon DynamoDB

## How to use
Add a following dependency into your `build.sbt` at first.

```scala
libraryDependencies += "jp.co.bizreach" %% "aws-dynamodb-scala" % "0.0.2"
```

Then you can use aws-dynamodb-scala in your code.

```scala
import jp.co.bizreach.dynamodb4s._
import awscala.dynamodbv2.DynamoDB

// for local environment
implicit val db = DynamoDB.local()

// for AWS environment
implicit val db = DynamoDB.apply(accessKeyId = "xxx", secretAccessKey = "xxx")
```

Create table definition as below:

```scala
object Members extends DynamoTable {
  protected val table = "members"
  val id      = DynamoHashKey[Int]("id")
  val country = DynamoRangeKey[String]("country")
  val name    = DynamoAttribute[String]("name")
  val age     = DynamoAttribute[Int]("age")
  val company = DynamoAttribute[String]("company")
}

// Case class is optional
case class Member(
  val id: Int,
  val country: String,
  val name: String,
  val age: Int,
  val company: Option[String]
)
```

### Put

```scala
// Put by case class
Members.put(Member(1, "Japan", "Naoki Takezoe", 30, Some("BizReach")))

// Update only specified properties
Members.putAttributes(1, "Japan"){ t =>
  t.name -> "Takako Shimamoto" :: t.age  -> 25 :: Nil
}
```

### Query

```scala
// Query with case class mapping
val list: Seq[Member] = Members.query.keyConditions { t =>
  t.id -> Condition.eq(1) :: t.country -> Condition.eq("Japan") :: Nil
}.as[Member]

// Query with manual mapping
val list: Seq[(String, Int)] = Members.query
  .attribute(_.name)
  .attribute(_.age)
  .keyCondition(_.id -> Condition.eq(1))
  .keyCondition(_.country -> Condition.eq("Japan"))
  .limit(100000)
  .map { (t, x) =>
    (x.get(t.name), x.get(t.age))
  }
```
