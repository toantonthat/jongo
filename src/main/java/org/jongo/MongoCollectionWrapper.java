/*
 * Copyright (C) 2011 Benoît GUÉROUT <bguerout at gmail dot com> and Yves AMSELLEM <amsellem dot yves at gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jongo;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertOneResult;
import org.bson.Document;
import org.bson.types.ObjectId;


public class MongoCollectionWrapper {

  public static final String MONGO_DOCUMENT_ID_NAME = "_id";
  public static final String MONGO_QUERY_OID = "$oid";
  private static final Object[] NO_PARAMETERS = {};
  private static final String ALL = "{}";

  private final MongoCollection<Document> collection;
  private final WriteConcern writeConcern;
  private final ReadPreference readPreference;
  private final ReadConcern readConcern;
  private final Mapper mapper;

  public MongoCollectionWrapper(MongoCollection<Document> collection, Mapper mapper) {
    this(collection, mapper, collection.getWriteConcern(), collection.getReadPreference(),
        collection.getReadConcern());
  }

  private MongoCollectionWrapper(MongoCollection<Document> collection, Mapper mapper,
      WriteConcern writeConcern, ReadPreference readPreference, ReadConcern readConcern) {
    this.collection = collection;
    this.writeConcern = writeConcern;
    this.readPreference = readPreference;
    this.mapper = mapper;
    this.readConcern = readConcern;
  }

  public MongoCollectionWrapper withWriteConcern(WriteConcern concern) {
    return new MongoCollectionWrapper(collection, mapper, concern, readPreference, readConcern);
  }

  public MongoCollectionWrapper withReadPreference(ReadPreference readPreference) {
    return new MongoCollectionWrapper(collection, mapper, writeConcern, readPreference,
        readConcern);
  }

  public MongoCollectionWrapper withReadConcern(ReadConcern readConcern) {
    return new MongoCollectionWrapper(collection, mapper, writeConcern, readPreference,
        readConcern);
  }

  public FindOne findOne(ObjectId id) {
    if (id == null) {
      throw new IllegalArgumentException("Object id must not be null");
    }
    return new FindOne(collection, readPreference, mapper.getUnmarshaller(),
        mapper.getQueryFactory(), "{_id:#}", id);
  }

//    public FindOne findOne() {
//        return findOne(ALL);
//    }

  public FindOne findOne(String query) {
    return findOne(query, NO_PARAMETERS);
  }

  public FindOne findOne(String query, Object... parameters) {
    return new FindOne(collection, readPreference, mapper.getUnmarshaller(),
        mapper.getQueryFactory(), query, parameters);
  }
//
//    public Find find() {
//        return find(ALL);
//    }
//
//    public Find find(String query) {
//        return find(query, NO_PARAMETERS);
//    }
//
//    public Find find(String query, Object... parameters) {
//        return new Find(collection, readPreference, mapper.getUnmarshaller(), mapper.getQueryFactory(), query, parameters);
//    }
//
//    public FindAndModify findAndModify() {
//        return findAndModify(ALL);
//    }
//
//    public FindAndModify findAndModify(String query) {
//        return findAndModify(query, NO_PARAMETERS);
//    }
//
//    public FindAndModify findAndModify(String query, Object... parameters) {
//        return new FindAndModify(collection, mapper.getUnmarshaller(), mapper.getQueryFactory(), query, parameters);
//    }

    public long count() {
        return collection.countDocuments();
    }
//
//    public long count(String query) {
//        return count(query, NO_PARAMETERS);
//    }
//
//    public long count(String query, Object... parameters) {
//        DBObject dbQuery = createQuery(query, parameters).toDBObject();
//        return collection.getCount(dbQuery);
//    }
//
//    public Update update(String query) {
//        return update(query, NO_PARAMETERS);
//    }
//
//    public Update update(ObjectId id) {
//        if (id == null) {
//            throw new IllegalArgumentException("Object id must not be null");
//        }
//        return update("{_id:#}", id);
//    }
//
//    public Update update(String query, Object... parameters) {
//        return new Update(collection, writeConcern, mapper.getQueryFactory(), query, parameters);
//    }

  public InsertOneResult save(Object pojo) {
    return new Insert(collection, writeConcern, mapper.getMarshaller(), mapper.getObjectIdUpdater(),
        mapper.getQueryFactory()).save(pojo);
  }
//
//    public WriteResult insert(Object pojo) {
//        return insert(new Object[]{pojo});
//    }
//
//    public WriteResult insert(String query) {
//        return insert(query, NO_PARAMETERS);
//    }
//
//    public WriteResult insert(Object... pojos) {
//        return new Insert(collection, writeConcern, mapper.getMarshaller(), mapper.getObjectIdUpdater(), mapper.getQueryFactory()).insert(pojos);
//    }
//
//    public WriteResult insert(String query, Object... parameters) {
//        return new Insert(collection, writeConcern, mapper.getMarshaller(), mapper.getObjectIdUpdater(), mapper.getQueryFactory()).insert(query, parameters);
//    }
//
//    public WriteResult remove(ObjectId id) {
//        return remove("{" + MONGO_DOCUMENT_ID_NAME + ":#}", id);
//    }
//
//    public WriteResult remove() {
//        return remove(ALL);
//    }
//
//    public WriteResult remove(String query) {
//        return remove(query, NO_PARAMETERS);
//    }
//
//    public WriteResult remove(String query, Object... parameters) {
//        return collection.remove(createQuery(query, parameters).toDBObject(), writeConcern);
//    }
//
//    public Distinct distinct(String key) {
//        return new Distinct(collection, mapper.getUnmarshaller(), mapper.getQueryFactory(), key);
//    }
//
//    public Aggregate aggregate(String pipelineOperator) {
//        return aggregate(pipelineOperator, NO_PARAMETERS);
//    }
//
//    public Aggregate aggregate(String pipelineOperator, Object... parameters) {
//        return new Aggregate(collection, mapper.getUnmarshaller(), mapper.getQueryFactory()).and(pipelineOperator, parameters);
//    }

    public void drop() {
        collection.drop();
    }
//
//    public void dropIndex(String keys) {
//        collection.dropIndex(createQuery(keys).toDBObject());
//    }
//
//    public void dropIndexes() {
//        collection.dropIndexes();
//    }
//
//    public void ensureIndex(String keys) {
//        collection.createIndex(createQuery(keys).toDBObject());
//    }
//
//    public void ensureIndex(String keys, String options) {
//        collection.createIndex(createQuery(keys).toDBObject(), createQuery(options).toDBObject());
//    }
//
//    public String getName() {
//        return collection.getName();
//    }
//
//    public DBCollection getDBCollection() {
//        return collection;
//    }
//
//    private Query createQuery(String query, Object... parameters) {
//        return mapper.getQueryFactory().createQuery(query, parameters);
//    }

  @Override
  public String toString() {
    if (collection != null) {
      MongoNamespace namespace = collection.getNamespace();
      return "collection {" + "name: '" + namespace.getCollectionName() + "', db: '"
          + namespace.getDatabaseName() + "'}";
    } else {
      return super.toString();
    }
  }
}
