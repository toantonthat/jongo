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

import org.bson.Document;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

class BsonDBObject extends BsonDocument {

  private final byte[] bytes;

  public BsonDBObject(byte[] data, int offset) {
    this.bytes = data; // Assuming offset is not needed for direct parsing
  }

  public byte[] toByteArray() {
    return bytes;
  }

  public Document toDocument() {
    // Parse bytes into a BsonDocument
    RawBsonDocument rawDoc = new RawBsonDocument(bytes);

    // Convert BsonDocument to Document
    return Document.parse(rawDoc.toJson());
  }

  public int getSize() {
    // Calculate BSON size if needed
    return bytes.length; // Example assuming bytes.length is the size
  }
}
