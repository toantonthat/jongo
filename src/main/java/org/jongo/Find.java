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

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.BsonDocument;
import org.bson.Document;
import org.jongo.marshall.Unmarshaller;
import org.jongo.query.Query;
import org.jongo.query.QueryFactory;
import org.jongo.ResultHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.jongo.ResultHandlerFactory.newResultHandler;

public class Find {

  private final MongoCollection<Document> collection;
  private final Unmarshaller unmarshaller;
  private final QueryFactory queryFactory;
  private final Query query;
  private final List<Consumer<FindIterable<Document>>> modifiers;
  private Query fields;

  Find(MongoCollection<Document> collection, Unmarshaller unmarshaller, QueryFactory queryFactory,
      String query, Object... parameters) {
    this.unmarshaller = unmarshaller;
    this.collection = collection;
    this.queryFactory = queryFactory;
    this.query = this.queryFactory.createQuery(query, parameters);
    this.modifiers = new ArrayList<>();
  }

  public <T> MongoCursor<T> as(final Class<T> clazz) {
    return map(newResultHandler(clazz, unmarshaller));
  }

  public <T> MongoCursor<T> map(ResultHandler<T> resultHandler) {
    BsonDocument filter = query.toBsonDocument();
    FindIterable<Document> findIterable = collection.find(filter)
        .projection(getFieldsAsBsonDocument());
    for (Consumer<FindIterable<Document>> modifier : modifiers) {
      modifier.accept(findIterable);
    }
    return new MongoCursorWrapper<T>(findIterable.iterator(), resultHandler);
  }

  public Find projection(String fields) {
    this.fields = queryFactory.createQuery(fields);
    return this;
  }

  public Find projection(String fields, Object... parameters) {
    this.fields = queryFactory.createQuery(fields, parameters);
    return this;
  }

  public Find limit(final int limit) {
    this.modifiers.add(findIterable -> findIterable.limit(limit));
    return this;
  }

  public Find skip(final int skip) {
    this.modifiers.add(findIterable -> findIterable.skip(skip));
    return this;
  }

  public Find sort(String sort) {
    final BsonDocument sortBsonDocument = queryFactory.createQuery(sort).toBsonDocument();
    this.modifiers.add(findIterable -> findIterable.sort(sortBsonDocument));
    return this;
  }

  public Find hint(String hint) {
    final BsonDocument hintBsonDocument = queryFactory.createQuery(hint).toBsonDocument();
    this.modifiers.add(findIterable -> findIterable.hint(hintBsonDocument));
    return this;
  }

  public Find with(Consumer<FindIterable<Document>> queryModifier) {
    this.modifiers.add(queryModifier);
    return this;
  }

  private BsonDocument getFieldsAsBsonDocument() {
    return fields == null ? null : fields.toBsonDocument();
  }

}
