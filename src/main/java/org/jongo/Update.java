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

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import com.mongodb.client.result.UpdateResult;
import org.bson.*;
import org.jongo.query.Query;
import org.jongo.query.QueryFactory;

public class Update {

  private final MongoCollection<Document> collection;
  private final Query query;
  private final QueryFactory queryFactory;

  private boolean upsert = false;

  public Update(MongoCollection<Document> collection, WriteConcern writeConcern,
      QueryFactory queryFactory, String query, Object... parameters) {
    this.collection = collection;
    this.queryFactory = queryFactory;
    this.query = createQuery(query, parameters);
  }

  public UpdateResult with(String modifier) {
    return with(modifier, new Object[0]);
  }

  public UpdateResult with(String modifier, Object... parameters) {
    Query updateQuery = queryFactory.createQuery(modifier, parameters);
    UpdateOptions options = new UpdateOptions().upsert(upsert);
    return collection.updateMany(this.query.toBsonDocument(), updateQuery.toBsonDocument(),
        options);
  }

  public UpdateResult with(Object pojo) {
    BsonDocument updateDocument = queryFactory.createQuery("{$set:#}", pojo).toBsonDocument();
    removeIdField(updateDocument);
    UpdateOptions options = new UpdateOptions().upsert(upsert);
    return collection.updateMany(this.query.toBsonDocument(), updateDocument, options);
  }

  private void removeIdField(BsonDocument updateDocument) {
    if (updateDocument.containsKey("$set")) {
      BsonDocument setDocument = updateDocument.getDocument("$set");
      if (setDocument.containsKey("_id")) {
        setDocument.remove("_id");
      }
    }
  }

  public Update upsert() {
    this.upsert = true;
    return this;
  }

  private Query createQuery(String query, Object[] parameters) {
    try {
      return this.queryFactory.createQuery(query, parameters);
    } catch (Exception e) {
      String message = String.format("Unable execute update operation using query %s", query);
      throw new IllegalArgumentException(message, e);
    }
  }
}
