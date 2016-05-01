package jp.co.bizreach.dynamodb4s

import DynamoTable._
import com.amazonaws.services.dynamodbv2.model.{Select, QueryRequest, Condition}
import scala.collection.JavaConverters._

import scala.reflect.ClassTag

case class DynamoQueryBuilder[T <: DynamoTable](
  private val _table: T,
  private val _keyConditions: T => Seq[(DynamoKey, com.amazonaws.services.dynamodbv2.model.Condition)],
  private val _attributes: T => Seq[DynamoProperty[_]],
  private val _limit: Int = 1000,
  private val _consistentRead: Boolean = false,
  private val _indexName: Option[String] = None){

  def secondaryIndexCondition[I <: SecondaryIndex](i: T  => I)(f: I => Seq[(DynamoKey, Condition)]): DynamoQueryBuilder[T] = {
    val index = i(_table)
    this.copy(_indexName = Some(index.index), _keyConditions = t => (_keyConditions(t) ++ f(index)))
  }

  def keyConditions(f: T => Seq[(DynamoKey, Condition)]): DynamoQueryBuilder[T] = this.copy(_keyConditions = t => (_keyConditions(t) ++ f(t)))

  def keyCondition(f: T => (DynamoKey, Condition)): DynamoQueryBuilder[T] = this.copy(_keyConditions = t => (_keyConditions(t) ++ Seq(f(t))))

  def attributes(f: T => Seq[DynamoProperty[_]]): DynamoQueryBuilder[T] = this.copy(_attributes = t => (_attributes(t) ++ f(t)))

  def attribute(f: T => DynamoProperty[_]): DynamoQueryBuilder[T] = this.copy(_attributes = t => (_attributes(t) ++ Seq(f(t))))

  def limit(i: Int): DynamoQueryBuilder[T] = this.copy(_limit = i)

  def consistentRead(b: Boolean): DynamoQueryBuilder[T] = this.copy(_consistentRead = b)

  def map[E](mapper: (T, DynamoRow) => E)(implicit db: awscala.dynamodbv2.DynamoDB): Seq[E] = {
    val req = new QueryRequest()
      .withTableName(_table.table)
      .withKeyConditions(_keyConditions(_table).map { case (key, condition) => key.name -> condition }.toMap.asJava)
      .withLimit(_limit)
      .withConsistentRead(_consistentRead)
      .withSelect(Select.SPECIFIC_ATTRIBUTES)
      .withAttributesToGet(_attributes(_table).map(_.name): _*)

    _indexName.foreach(req.withIndexName)

    val items  = db.query(req).getItems
    items.asScala.map { item =>
      mapper(_table, new DynamoRow(item))
    }
  }

  def list[E <: AnyRef](implicit db: awscala.dynamodbv2.DynamoDB, c: ClassTag[E]): Seq[E] = as

  def firstOption[E <: AnyRef](implicit db: awscala.dynamodbv2.DynamoDB, c: ClassTag[E]): Option[E] = as.headOption

  def first[E <: AnyRef](implicit db: awscala.dynamodbv2.DynamoDB, c: ClassTag[E]): E = as.head

  def as[E <: AnyRef](implicit db: awscala.dynamodbv2.DynamoDB, c: ClassTag[E]): Seq[E] = {
    val req = new QueryRequest()
      .withTableName(_table.table)
      .withKeyConditions(_keyConditions(_table).map { case (key, condition) => key.name -> condition }.toMap.asJava)
      .withLimit(_limit)
      .withConsistentRead(_consistentRead)
      .withSelect(Select.SPECIFIC_ATTRIBUTES)
      .withAttributesToGet(c.runtimeClass.getDeclaredFields.map(_.getName): _*)

    _indexName.foreach(req.withIndexName)

    val items  = db.query(req).getItems
    val tableInfo = getTableInfo(_table)
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
        val attribute = x.get(f.getName)
        val property = tableInfo.getDynamoProperty(f.getName)
        if(t == classOf[Option[_]]){
          f.set(o, Option(getAttributeValue(attribute)).map(property.convert))
        } else {
          f.set(o, property.convert(getAttributeValue(attribute)))
        }
      }
      o.asInstanceOf[E]
    }
  }
}