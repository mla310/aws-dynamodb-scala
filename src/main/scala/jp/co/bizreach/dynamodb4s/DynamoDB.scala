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

  /**
   * Delete item by the hash key.
   */
  def delete(hashPk: Any)(implicit db: awscala.dynamodbv2.DynamoDB): Unit = {
    db.deleteItem(db.table(table).get, hashPk)
  }

  /**
   * Delete item by the hash key and the range key.
   */
  def delete(hashPk: Any, rangePk: Any)(implicit db: awscala.dynamodbv2.DynamoDB): Unit = {
    db.deleteItem(db.table(table).get, hashPk, rangePk)
  }

  /**
   * Create or update the given item
   */
  def put[E](entity: E)(implicit db: awscala.dynamodbv2.DynamoDB, t: TypeTag[E], c: ClassTag[E]): Unit = {
    val mapper = new AnnotationMapper[E]

    val hashPkSymbol  = mapper.getAs[hashPk]
    val rangePkSymbol = mapper.getAs[rangePk]
    val attrSymbols   = mapper.notAnnotatedMemberSymbols
    val attributes    = attrSymbols.map { s => s.name.toString -> mapper.getValue(entity, s) }

    if(hashPkSymbol.isDefined && rangePkSymbol.isDefined){
      db.put(db.table(table).get, mapper.getValue(entity, hashPkSymbol.get), mapper.getValue(entity, rangePkSymbol.get), attributes: _*)
    } else if(hashPkSymbol.isDefined){
      db.put(db.table(table).get, mapper.getValue(entity, hashPkSymbol.get), attributes: _*)
    } else {
      // TODO Error!!
      println("ERROR!!")
    }
  }

  /**
   * Update specified attributes by the hash key.
   */
  def putAttributes(hashPk: Any)(f: T => Seq[(DynamoAttribute, Any)])(implicit db: awscala.dynamodbv2.DynamoDB): Unit = {
    db.table(table).get.putAttributes(hashPk, f(this).map { case (key, value) => (key.name, value) })
  }

  /**
   * Update specified attributes by the hash key and the range key.
   */
  def putAttributes(hashPk: Any, rangePk: Any)(f: T => Seq[(DynamoAttribute, Any)])(implicit db: awscala.dynamodbv2.DynamoDB): Unit = {
    db.table(table).get.putAttributes(hashPk, rangePk, f(this).map { case (key, value) => (key.name, value) })
  }

  /**
   *
   */
  def query[E](keyConditions: Seq[(String, com.amazonaws.services.dynamodbv2.model.Condition)],
               limit: Int = 1000,
               consistentRead: Boolean = false
              )(implicit db: awscala.dynamodbv2.DynamoDB, t: TypeTag[E], c: ClassTag[E]): Seq[E] = {
    val mapper = new AnnotationMapper[E]

    val req = new QueryRequest()
      .withTableName(table)
      .withKeyConditions(keyConditions.toMap.asJava)
      .withLimit(limit)
      .withConsistentRead(consistentRead)
      .withSelect(Select.SPECIFIC_ATTRIBUTES)
      .withAttributesToGet(mapper.memberSymbols.map(_.name.toString): _*)

    val items  = db.query(req).getItems
    val clazz  = c.runtimeClass
    val fields = clazz.getDeclaredFields

    items.asScala.map { x =>
      val c = clazz.getConstructors()(0)
      val args = c.getParameterTypes.map { x =>
        if(x == classOf[Int]) new Integer(0) else null
      }
      val o = c.newInstance(args: _*)
      fields.foreach { f =>
        f.setAccessible(true)
        val t = f.getType
        val attr = x.get(f.getName)
        if(t == classOf[Option[_]]){
          f.set(o, Option(getAttributeValue(attr)))
        } else {
          f.set(o, getAttributeValue(attr))
        }
      }
      o.asInstanceOf[E]
    }
  }

  private def getAttributeValue(attr: AttributeValue): Any = {
    if(attr.getB != null){
      attr.getB
    } else if(attr.getBS != null){
      attr.getBS
    } else if(attr.getN != null){
      attr.getN.toInt
    } else if(attr.getNS != null){
      attr.getNS.asScala.map(_.toInt)
    } else if(attr.getS != null){
      attr.getS
    } else if(attr.getSS != null){
      attr.getSS.asScala
    } else {
      null
    }
  }

}

//case class HashKey(name: String)
//case class RangeKey(name: String)
case class DynamoAttribute(name: String)

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