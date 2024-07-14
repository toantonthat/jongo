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

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import java.util.concurrent.TimeUnit;
import org.bson.*;
import org.bson.types.ObjectId;
import org.jongo.bson.BsonDocument;
import org.jongo.marshall.Marshaller;
import org.jongo.query.QueryFactory;

import java.util.ArrayList;
import java.util.List;

public class Insert {

  private final Marshaller marshaller;
  private final MongoCollection<Document> collection;
  private final ObjectIdUpdater objectIdUpdater;
  private final QueryFactory queryFactory;
  private final WriteConcern writeConcern;

  public Insert(MongoCollection<Document> collection, WriteConcern writeConcern,
      Marshaller marshaller, ObjectIdUpdater objectIdUpdater, QueryFactory queryFactory) {
    this.writeConcern = writeConcern;
    this.marshaller = marshaller;
    this.collection = collection;
    this.objectIdUpdater = objectIdUpdater;
    this.queryFactory = queryFactory;
  }

  public InsertOneResult insertOne(Object pojo, String query, Object... parameters) {
    Object id = preparePojo(pojo);
    Document document = convertToDocument(pojo, id);
    // Create the query document
    Document dbQuery = queryFactory.createQuery(query, parameters).toDocument();

    // Merge query fields into the document to insert
    document.putAll(dbQuery);

    return collection.insertOne(document);
  }

//  public Document convertWriteConcernToDocument() {
//    Document writeConcernDoc = new Document();
//    writeConcernDoc.append("w", writeConcern.getW())
//        .append("wtimeout", writeConcern.getWTimeout(TimeUnit.MILLISECONDS))
//        .append("journal", writeConcern.getJournal())
//        .append("fsync", writeConcern.isFsync());
//
//    return writeConcernDoc;
//  }

//  public WriteResult insert(String query, Object... parameters) {
//    DBObject dbQuery = queryFactory.createQuery(query, parameters).toDBObject();
//    if (dbQuery instanceof BasicDBList) {
//      return insert(((BasicDBList) dbQuery).toArray());
//    } else {
//      return collection.insert(dbQuery, writeConcern);
//    }
//  }

  public InsertOneResult save(Object pojo) {
    Object id = preparePojo(pojo);
    Document document = convertToDocument(pojo, id);
    return collection.insertOne(document);
  }

  public void insert(Object... pojos) {
    List<Document> documents = new ArrayList<>(pojos.length);
    for (Object pojo : pojos) {
      Object id = preparePojo(pojo);
      Document document = convertToDocument(pojo, id);
      documents.add(document);
    }
    collection.insertMany(documents);
  }

  public void insert(String query, Object... parameters) {
    Document dbQuery = queryFactory.createQuery(query, parameters).toDocument();
    collection.insertOne(dbQuery);
  }

  private Object preparePojo(Object pojo) {
    if (objectIdUpdater.mustGenerateObjectId(pojo)) {
      ObjectId newOid = new ObjectId();
      objectIdUpdater.setObjectId(pojo, newOid);
      return newOid;
    }
    return objectIdUpdater.getId(pojo);
  }

  private Document convertToDocument(Object pojo, Object id) {
    BsonDocument bsonDocument = asBsonDocument(marshaller, pojo);
    Document document = Document.parse(bsonDocument.toDocument().toJson());
    if (id != null) {
      document.put("_id", id);
    }
    return document;
  }

  private static BsonDocument asBsonDocument(Marshaller marshaller, Object obj) {
    try {
      return marshaller.marshall(obj);
    } catch (Exception e) {
      String message = String.format("Unable to save object %s due to a marshalling error", obj);
      throw new IllegalArgumentException(message, e);
    }
  }
}
