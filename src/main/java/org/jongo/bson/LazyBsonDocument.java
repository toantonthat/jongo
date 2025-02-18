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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.bson.Document;
import org.bson.RawBsonDocument;

class LazyBsonDocument implements BsonDocument {

  private final byte[] bytes;

  LazyBsonDocument(byte[] bytes) {
    this.bytes = bytes;
  }

  public int getSize() {
    return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt(0);
  }

  public byte[] toByteArray() {
    return bytes;
  }

//    public DBObject toDBObject() {
//        return new BsonDBObject(bytes, 0);
//    }

  public Document toDocument() {
    RawBsonDocument rawDoc = new RawBsonDocument(bytes);
    return Document.parse(rawDoc.toJson()); // Assuming bytes represent JSON data
  }

  @Override
  public String toString() {
    return toDocument().toJson();
  }

}
