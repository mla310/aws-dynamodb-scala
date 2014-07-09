package jp.co.bizreach.dynamodb4s

import com.amazonaws.services.dynamodbv2.model.{QueryRequest, Select, AttributeValue, ExpectedAttributeValue}
//import awscala.Condition
//import awscala.dynamodbv2.{DynamoDB, Condition}
import com.amazonaws.services.dynamodbv2.model.Condition
import scala.collection.JavaConverters._

import reflect.ClassTag
import reflect.runtime._
import universe._

/**
 * Trait for Dynamo table definition.
 */
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
      throw new DynamoDBException(s"Primary key is not defined for ${entity.getClass.getName}")
    }
  }

  /**
   * Update specified attributes by the hash key.
   */
  def putAttributes(hashPk: Any)(f: T => Seq[(DynamoAttribute[_], Any)])(implicit db: awscala.dynamodbv2.DynamoDB): Unit = {
    db.table(table).get.putAttributes(hashPk, f(this).map { case (key, value) => (key.name, value) })
  }

  /**
   * Update specified attributes by the hash key and the range key.
   */
  def putAttributes(hashPk: Any, rangePk: Any)(f: T => Seq[(DynamoAttribute[_], Any)])(implicit db: awscala.dynamodbv2.DynamoDB): Unit = {
    db.table(table).get.putAttributes(hashPk, rangePk, f(this).map { case (key, value) => (key.name, value) })
  }

  def query(): MapBuilder[T] = MapBuilder(this, t => Nil, t => Nil)

  case class MapBuilder[T](
    private val _table: T,
    private val _keyConditions: T => Seq[(DynamoKey, com.amazonaws.services.dynamodbv2.model.Condition)],
    private val _attributes: T => Seq[DynamoAttribute[_]],
    private val _limit: Int = 1000,
    private val _consistentRead: Boolean = false){

    def keyConditions(f: T => Seq[(DynamoKey, Condition)]): MapBuilder[T] = this.copy(_keyConditions = t => (_keyConditions(t) ++ f(t)))

    def keyCondition(f: T => (DynamoKey, Condition)): MapBuilder[T] = this.copy(_keyConditions = t => (_keyConditions(t) ++ Seq(f(t))))

    def attributes(f: T => Seq[DynamoAttribute[_]]): MapBuilder[T] = this.copy(_attributes = t => (_attributes(t) ++ f(t)))

    def attribute(f: T => DynamoAttribute[_]): MapBuilder[T] = this.copy(_attributes = t => (_attributes(t) ++ Seq(f(t))))

    def limit(i: Int): MapBuilder[T] = this.copy(_limit = i)

    def consistentRead(b: Boolean): MapBuilder[T] = this.copy(_consistentRead = b)

    def map[E](mapper: (T, DynamoRow) => E)(implicit db: awscala.dynamodbv2.DynamoDB): Seq[E] = {
      val req = new QueryRequest()
        .withTableName(table)
        .withKeyConditions(_keyConditions(_table).map { case (key, condition) => key.name -> condition }.toMap.asJava)
        .withLimit(_limit)
        .withConsistentRead(_consistentRead)
        .withSelect(Select.SPECIFIC_ATTRIBUTES)
        .withAttributesToGet(_attributes(_table).map(_.name): _*)

      val items  = db.query(req).getItems
      items.asScala.map { item =>
        mapper(_table, new DynamoRow(item))
      }
    }
  }

  class DynamoRow(attrs: java.util.Map[String, AttributeValue]){
    def get[T](attr: DynamoAttribute[T]) = getAttributeValue(attrs.get(attr.name)).asInstanceOf[T]
  }

  def query[E](keyConditions: T => Seq[(DynamoKey, com.amazonaws.services.dynamodbv2.model.Condition)],
               limit: Int = 1000,
               consistentRead: Boolean = false
              )(implicit db: awscala.dynamodbv2.DynamoDB, t: TypeTag[E], c: ClassTag[E]): Seq[E] = {
    val mapper = new AnnotationMapper[E]

    val req = new QueryRequest()
      .withTableName(table)
      .withKeyConditions(keyConditions(this).map { case (key, condition) => key.name -> condition }.toMap.asJava)
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
