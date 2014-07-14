package jp.co.bizreach.dynamodb4s

import com.amazonaws.services.dynamodbv2.model.{QueryRequest, Select, AttributeValue}
import com.amazonaws.services.dynamodbv2.model.Condition
import scala.collection.JavaConverters._

import reflect.ClassTag
import reflect.runtime._
import universe._
import DynamoTable._

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
  def put[E <: AnyRef](entity: E)(implicit db: awscala.dynamodbv2.DynamoDB, t: TypeTag[E], c: ClassTag[E]): Unit = {
    val tableInfo = getTableInfo(this)

    if(tableInfo.hashKey.isDefined && tableInfo.rangeKey.isDefined){
      db.put(db.table(table).get,
        getValueFromEntity(entity, tableInfo.hashKey.get),
        getValueFromEntity(entity, tableInfo.rangeKey.get),
        tableInfo.attributes.map(p => p.name -> getValueFromEntity(entity, p)): _*)
    } else if(tableInfo.hashKey.isDefined){
      db.put(db.table(table).get,
        getValueFromEntity(entity, tableInfo.hashKey.get),
        tableInfo.attributes.map(p => p.name -> getValueFromEntity(entity, p)): _*)
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

  def query(): DynamoQueryBuilder[T] = DynamoQueryBuilder(this, t => Nil, t => Nil)

  case class DynamoQueryBuilder[T <: DynamoTable](
    private val _table: T,
    private val _keyConditions: T => Seq[(DynamoKey, com.amazonaws.services.dynamodbv2.model.Condition)],
    private val _attributes: T => Seq[DynamoProperty[_]],
    private val _limit: Int = 1000,
    private val _consistentRead: Boolean = false){

    def keyConditions(f: T => Seq[(DynamoKey, Condition)]): DynamoQueryBuilder[T] = this.copy(_keyConditions = t => (_keyConditions(t) ++ f(t)))

    def keyCondition(f: T => (DynamoKey, Condition)): DynamoQueryBuilder[T] = this.copy(_keyConditions = t => (_keyConditions(t) ++ Seq(f(t))))

    def attributes(f: T => Seq[DynamoProperty[_]]): DynamoQueryBuilder[T] = this.copy(_attributes = t => (_attributes(t) ++ f(t)))

    def attribute(f: T => DynamoProperty[_]): DynamoQueryBuilder[T] = this.copy(_attributes = t => (_attributes(t) ++ Seq(f(t))))

    def limit(i: Int): DynamoQueryBuilder[T] = this.copy(_limit = i)

    def consistentRead(b: Boolean): DynamoQueryBuilder[T] = this.copy(_consistentRead = b)

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

    def as[E <: AnyRef](implicit db: awscala.dynamodbv2.DynamoDB, c: ClassTag[E], t: TypeTag[E]): Seq[E] = {
      val req = new QueryRequest()
        .withTableName(table)
        .withKeyConditions(_keyConditions(_table).map { case (key, condition) => key.name -> condition }.toMap.asJava)
        .withLimit(_limit)
        .withConsistentRead(_consistentRead)
        .withSelect(Select.SPECIFIC_ATTRIBUTES)
        .withAttributesToGet(c.runtimeClass.getDeclaredFields.map(_.getName): _*)

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
            f.set(o, Option(property.convert(getAttributeValue(attribute))))
          } else {
            f.set(o, property.convert(getAttributeValue(attribute)))
          }
        }
        o.asInstanceOf[E]
      }
    }
  }
}

object DynamoTable {

  class DynamoRow(attrs: java.util.Map[String, AttributeValue]){
    def get[T](property: DynamoProperty[T]) = property.convert(getAttributeValue(attrs.get(property.name)))
  }

  case class TableInfo(hashKey: Option[DynamoHashKey[_]],
                       rangeKey: Option[DynamoRangeKey[_]],
                       attributes: Seq[DynamoAttribute[_]]){

    def getDynamoProperty(name: String): DynamoProperty[_] =
      (Seq[Option[DynamoProperty[_]]](hashKey, rangeKey).flatten ++ attributes).find(_.name == name).get
  }

  private def getTableInfo(table: DynamoTable): TableInfo = {
    var hashKey: Option[DynamoHashKey[_]] = None
    var rangeKey: Option[DynamoRangeKey[_]] = None
    var attributes: scala.collection.mutable.ListBuffer[DynamoAttribute[_]]
      = new scala.collection.mutable.ListBuffer[DynamoAttribute[_]]

    table.getClass.getDeclaredFields.foreach { f =>
      f.setAccessible(true)
      if(f.getType.isAssignableFrom(classOf[DynamoHashKey[_]])){
        hashKey = Some(f.get(table).asInstanceOf[DynamoHashKey[_]])
      }
      else if(f.getType.isAssignableFrom(classOf[DynamoRangeKey[_]])){
        rangeKey = Some(f.get(table).asInstanceOf[DynamoRangeKey[_]])
      }
      else if(f.getType.isAssignableFrom(classOf[DynamoAttribute[_]])){
        attributes += f.get(table).asInstanceOf[DynamoAttribute[_]]
      }
    }

    TableInfo(hashKey, rangeKey, attributes.toSeq)
  }

  private def getValueFromEntity(entity: AnyRef, property: DynamoProperty[_]): Any = {
    val f = entity.getClass.getDeclaredField(property.name)
    f.setAccessible(true)
    property.convert(f.get(entity))
  }

  private def getAttributeValue(attr: AttributeValue): Any = {
    // TODO Support binary type
    //    if(attr.getB != null){
    //      attr.getB
    //    } else if(attr.getBS != null){
    //      attr.getBS
    //    } else
    if(attr.getN != null){
      attr.getN
    } else if(attr.getNS != null){
      attr.getNS.asScala
    } else if(attr.getS != null){
      attr.getS
    } else if(attr.getSS != null){
      attr.getSS.asScala
    } else {
      null
    }
  }
}
