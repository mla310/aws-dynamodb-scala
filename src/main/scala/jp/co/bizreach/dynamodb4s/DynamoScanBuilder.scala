package jp.co.bizreach.dynamodb4s

import com.amazonaws.services.dynamodbv2.model.{QueryRequest, ScanRequest, Select}
import jp.co.bizreach.dynamodb4s.DynamoTable._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

case class DynamoScanBuilder[T <: DynamoTable](
  private val _table: T,
  private val _attributes: T => Seq[DynamoProperty[_]],
  private val _filterExpression: Option[String] = None,
  private val _limit: Int = 1000,
  private val _consistentRead: Boolean = false){

  def attributes(f: T => Seq[DynamoProperty[_]]): DynamoScanBuilder[T] = this.copy(_attributes = t => (_attributes(t) ++ f(t)))

  def attribute(f: T => DynamoProperty[_]): DynamoScanBuilder[T] = this.copy(_attributes = t => (_attributes(t) ++ Seq(f(t))))

  def limit(i: Int): DynamoScanBuilder[T] = this.copy(_limit = i)

  def filterExpression(expression: String): DynamoScanBuilder[T] = this.copy(_filterExpression = Some(expression))

  def consistentRead(b: Boolean): DynamoScanBuilder[T] = this.copy(_consistentRead = b)

  def map[E](mapper: (T, DynamoRow) => E)(implicit db: awscala.dynamodbv2.DynamoDB): Seq[E] = {
    val req = new ScanRequest()
      .withTableName(_table.table)
      .withLimit(_limit)
      .withConsistentRead(_consistentRead)
      .withSelect(Select.SPECIFIC_ATTRIBUTES)
      .withAttributesToGet(_attributes(_table).map(_.name): _*)

    _filterExpression.foreach(req.withFilterExpression)

    val items  = db.scan(req).getItems
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
      .withLimit(_limit)
      .withConsistentRead(_consistentRead)
      .withSelect(Select.SPECIFIC_ATTRIBUTES)
      .withAttributesToGet(c.runtimeClass.getDeclaredFields.map(_.getName): _*)

    _filterExpression.foreach(req.withFilterExpression)

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
