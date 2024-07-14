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

import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class CustomMongoCursor<E> implements Iterator<E>, Iterable<E>, Closeable {

  private final MongoCursor<Document> cursor;
  private final ResultHandler<E> resultHandler;

  public CustomMongoCursor(MongoCursor<Document> cursor, ResultHandler<E> resultHandler) {
    this.cursor = cursor;
    this.resultHandler = resultHandler;
  }

  public boolean hasNext() {
    return cursor.hasNext();
  }

  public E next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    Document document = cursor.next();
    return resultHandler.map(document);
  }

  public void remove() {
    throw new UnsupportedOperationException("remove() method is not supported");
  }

  public Iterator<E> iterator() {
    return new CustomMongoCursor<E>(cursor, resultHandler);
  }

  public void close() {
    cursor.close();
  }

  public int count() {
    throw new UnsupportedOperationException(
        "count() method is not supported in this implementation");
  }
}
