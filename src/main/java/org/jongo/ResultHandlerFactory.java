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

import com.mongodb.DBObject;
import org.bson.Document;
import org.jongo.bson.Bson;
import org.jongo.bson.BsonDocument;
import org.jongo.marshall.Unmarshaller;

class ResultHandlerFactory {

  /**
   * Creates a new ResultHandler instance for mapping DBObject results to objects of type T.
   *
   * @param clazz        The class type to which the result should be mapped.
   * @param unmarshaller The unmarshaller to use for converting BSON documents to Java objects.
   * @param <T>          The type of the result.
   * @return A new ResultHandler instance.
   */
  public static <T> ResultHandler<T> newResultHandler(final Class<T> clazz,
      final Unmarshaller unmarshaller) {
    return new UnmarshallingResultHandler<T>(unmarshaller, clazz);
  }

  /**
   * Implementation of ResultHandler that performs unmarshalling of DBObject results.
   *
   * @param <T> The type of the result.
   */
  private static class UnmarshallingResultHandler<T> implements ResultHandler<T> {

    private final Unmarshaller unmarshaller;
    private final Class<T> clazz;

    public UnmarshallingResultHandler(Unmarshaller unmarshaller, Class<T> clazz) {
      this.unmarshaller = unmarshaller;
      this.clazz = clazz;
    }

    /**
     * Maps a DBObject result to an object of type T using the provided unmarshaller.
     *
     * @param result The DBObject result to map.
     * @return The mapped object of type T.
     */
    public T map(DBObject result) {
      BsonDocument bsonDocument = Bson.createDocument(result); // Convert DBObject to BsonDocument
      return unmarshaller.unmarshall(bsonDocument,
          clazz); // Unmarshall BsonDocument to object of type T
    }

    @Override
    public T map(Document result) {
      BsonDocument bsonDocument = Bson.createDocument(result);
      return unmarshaller.unmarshall(bsonDocument, clazz);
    }
  }

  // Private constructor to prevent instantiation of this factory class.
  private ResultHandlerFactory() {
  }
}
