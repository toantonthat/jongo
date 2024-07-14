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

package org.jongo.bson;

import com.mongodb.DBObject;
import org.bson.Document;

public class Bson {

  public static boolean isPrimitive(Object obj) {
    return Primitives.contains(obj.getClass());
  }

  public static BsonDocument createDocument(DBObject dbo) {
    // Convert DBObject to Document
    Document document = new Document();
    for (String key : dbo.keySet()) {
      document.append(key, dbo.get(key));
    }
    return new BufferedBsonDocument(document);
  }

  public static BsonDocument createDocument(Document document) {
    return new BufferedBsonDocument(document);
  }

  public static BsonDocument createDocument(byte[] bytes) {
    return new LazyBsonDocument(bytes);
  }


  private Bson() {
  }
}
