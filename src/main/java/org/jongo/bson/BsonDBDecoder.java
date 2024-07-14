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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.gridfs.model.GridFSFile;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;

import java.util.Iterator;

public class BsonDBDecoder {

  public static final BsonDBDecoderFactory FACTORY = new BsonDBDecoderFactory();

  private BsonDBDecoder() {
  }

  public Document decode(byte[] data, MongoCollection<Document> collection) {
    CollectionCallback callback = new CollectionCallback(collection);
    return callback.createObject(data);
  }

  private static class BsonDBDecoderFactory {

    public BsonDBDecoder create() {
      return new BsonDBDecoder();
    }
  }

  private static class CollectionCallback {

    private final MongoCollection<Document> collection;

    public CollectionCallback(MongoCollection<Document> collection) {
      this.collection = collection;
    }

    public Document createObject(byte[] data) {
      BsonBinaryReader bsonReader = new BsonBinaryReader(ByteBuffer.wrap(data));
      Document document = new DocumentCodec().decode(bsonReader, DecoderContext.builder().build());
//      Document document = new DocumentCodec().decode(new BasicInputBuffer(data), null);

      if (isGridFSCollection()) {
        return document; // No need to decode for GridFS, just return the document
      }

      if (isDBRef(document)) {
        Document refDoc = new Document();
        refDoc.append("$ref", document.get("$ref"));
        refDoc.append("$id", document.get("$id"));
        return refDoc;
      }

      return document;
    }

    private boolean isGridFSCollection() {
      return GridFSFile.class.equals(collection.getDocumentClass());
    }

    private boolean isDBRef(Document document) {
      Iterator<String> iterator = document.keySet().iterator();
      return iterator.hasNext() && iterator.next().equals("$ref") && iterator.hasNext()
          && iterator.next().equals("$id");
    }
  }
}
