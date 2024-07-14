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

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.DBObject;
import org.bson.Document;
import org.jongo.marshall.Unmarshaller;
import org.jongo.query.QueryFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import static org.jongo.ResultHandlerFactory.newResultHandler;

public class Aggregate {

  private final Unmarshaller unmarshaller;
  private final QueryFactory queryFactory;
  private final List<Document> pipeline;
  private final AtomicReference<AggregateIterable<Document>> options;
  private final MongoCollection<Document> collection;

  Aggregate(MongoCollection<Document> collection, Unmarshaller unmarshaller,
      QueryFactory queryFactory) {
    this.unmarshaller = unmarshaller;
    this.queryFactory = queryFactory;
    this.pipeline = new ArrayList<Document>();
    this.options = new AtomicReference<>();
    this.collection = collection;
  }

  public Aggregate and(String pipelineOperator, Object... parameters) {
    Document dbQuery = queryFactory.createQuery(pipelineOperator, parameters).toDocument();
    pipeline.add(dbQuery);
    return this;
  }

  public <T> ResultsIterator<T> as(final Class<T> clazz) {
    return map(newResultHandler(clazz, unmarshaller));
  }

  public Aggregate options(AggregateIterable<Document> options) {
    this.options.set(options);
    return this;
  }

  public <T> ResultsIterator<T> map(ResultHandler<T> resultHandler) {
    AggregateIterable<Document> results;
    AggregateIterable<Document> options = this.options.get();
//        if (options != null) {
//            results = collection.aggregate(pipeline, options);
//        } else {
//            results = collection.aggregate(pipeline, AggregationOptions.builder().build());
//        }

    results = collection.aggregate(pipeline);
    return new ResultsIterator<T>(results.iterator(), resultHandler);
  }

  public static class ResultsIterator<E> implements Iterator<E>, Iterable<E>, Closeable {

    private final MongoCursor<Document> results;
    private ResultHandler<E> resultHandler;

    private ResultsIterator(MongoCursor<Document> results, ResultHandler<E> resultHandler) {
      this.resultHandler = resultHandler;
      this.results = results;
    }

    public Iterator<E> iterator() {
      return this;
    }

    public boolean hasNext() {
      return results.hasNext();
    }

    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

      Document document = results.next();
      return resultHandler.map(document);
    }

    public void remove() {
      throw new UnsupportedOperationException("remove() method is not supported");
    }

    public void close() throws IOException {
      if (results instanceof Closeable) {
        Closeable closeable = (Closeable) results;
        closeable.close();
      }
    }

    boolean isCursor() {
      return (results instanceof MongoCursor);
    }
  }
}
