package jp.co.bizreach.dynamodb4s

import com.amazonaws.services.dynamodbv2.model.{QueryRequest, Select, AttributeValue, ExpectedAttributeValue}
import awscala.Condition
import awscala.dynamodbv2.{DynamoDB, Condition}
import scala.collection.JavaConverters._

import reflect.ClassTag
import reflect.runtime._
import universe._

trait DynamoTable {

  protected val table: String
  type T = this.type

  def delete(hashPk: Any)(implicit db: awscala.dynamodbv2.DynamoDB): Unit = {
    db.deleteItem(db.table(table).get, hashPk)
  }

  def delete(hashPk: Any, rangePk: Any)(implicit db: awscala.dynamodbv2.DynamoDB): Unit = {
    db.deleteItem(db.table(table).get, hashPk, rangePk)
  }

  def put[ENTITY](entity: ENTITY)(implicit db: awscala.dynamodbv2.DynamoDB, t: TypeTag[ENTITY], c: ClassTag[ENTITY]): Unit = {
    val mapper = new AnnotationMapper[ENTITY]
    val hashPkSymbol  = mapper.getAs[hashPk]
    val rangePkSymbol = mapper.getAs[rangePk]
    val attrSymbols   = mapper.notAnnotatedMemberSymbol

    val attributes = attrSymbols.map { s =>
      val name = s match {
        case Literal(Constant(name: String)) => name
      }
      name -> mapper.getValue(entity, s)
    }


//    if(hashPkSymbol.isDefined && rangePkSymbol.isDefined){
//      db.put(db.table(table).get,
//        mapper.getValue(entity, hashPkSymbol.get),
//        mapper.getValue(entity, rangePkSymbol.get),
//        attributes: _*)
//    } else if(hashPkSymbol.isDefined){
//      db.put(db.table(table).get,
//        mapper.getValue(entity, hashPkSymbol.get),
//        attributes: _*)
//    } else {
//      // TODO Error!!
//      println("ERROR!!")
//    }
  }

//  def putWithCondition(entity: ENTITY)(cond: T => Seq[(DynamoAttribute, ExpectedAttributeValue)])(implicit db: awscala.dynamodbv2.DynamoDB): Unit = {
//
//  }

  def putAttributes(hashPk: Any)(f: T => Seq[(DynamoAttribute, Any)])(implicit db: awscala.dynamodbv2.DynamoDB): Unit = {
    db.table(table).get.putAttributes(hashPk, f(this).map { case (key, value) => (key.name, value) })
  }

  def putAttributes(hashPk: Any, rangePk: Any)(f: T => Seq[(DynamoAttribute, Any)])(implicit db: awscala.dynamodbv2.DynamoDB): Unit = {
    db.table(table).get.putAttributes(hashPk, rangePk, f(this).map { case (key, value) => (key.name, value) })
  }

  def query[E](keyConditions: Seq[(String, com.amazonaws.services.dynamodbv2.model.Condition)],
               limit: Int = 1000,
               consistentRead: Boolean = false
              )(implicit db: awscala.dynamodbv2.DynamoDB): Seq[E] = {

    val req = new QueryRequest()
      .withTableName(table)
      .withKeyConditions(keyConditions.toMap.asJava)
      .withLimit(limit)
      .withConsistentRead(consistentRead)
      .withSelect(Select.SPECIFIC_ATTRIBUTES)
      .withAttributesToGet("", "")

    // TODO なんとかする
    val items = db.query(req).getItems

    null
  }

}

case class HashKey(name: String)
case class RangeKey(name: String)
case class DynamoAttribute(name: String)

object Members extends DynamoTable {
  protected val table = "Members"
  val name    = DynamoAttribute("Name")
  val age     = DynamoAttribute("Age")
  val company = DynamoAttribute("Company")
}

case class Member(id: Int, country: String, name: String, age: Int, company: String)

object test {

  /**
   * Implicit conversion from awscala.dynamodbv2.Condition to ExpectedAttributeValue for conditional updating
   */
  implicit def condition2expected(cond: com.amazonaws.services.dynamodbv2.model.Condition): ExpectedAttributeValue = {
    new ExpectedAttributeValue().withComparisonOperator(cond.getComparisonOperator).withAttributeValueList(cond.getAttributeValueList)
  }

  implicit val db = DynamoDB.local()

  Members.putAttributes(1, "Japan"){ t =>
    Seq(t.company -> "BizReach")
  }

//  Members.putWithCondition(Member(1, "Japan", "Chris", 32, "Google")){ t =>
//    Seq(t.company -> Condition.eq(0))
//  }


}