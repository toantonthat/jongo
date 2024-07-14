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

import org.bson.BsonBinaryWriter;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.DocumentCodec;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;

class BufferedBsonDocument implements BsonDocument {

  private final OutputBuffer buffer;
  private final Document document;

  BufferedBsonDocument(Document document) {
    this.buffer = new BasicOutputBuffer();
    this.document = document;
    encode(this.document);
  }

  private void encode(Document document) {
    CodecRegistry codecRegistry = org.bson.codecs.configuration.CodecRegistries.fromRegistries(
        org.bson.codecs.configuration.CodecRegistries.fromProviders(
            new org.bson.codecs.DocumentCodecProvider(),
            new org.bson.codecs.BsonValueCodecProvider(),
            new org.bson.codecs.ValueCodecProvider()
        )
    );
    DocumentCodec documentCodec = new DocumentCodec(codecRegistry);
    BsonWriter writer = new BsonBinaryWriter(buffer);
    documentCodec.encode(writer, document, EncoderContext.builder().build());
  }

  public int getSize() {
    return buffer.size();
  }

  public byte[] toByteArray() {
    return buffer.toByteArray();
  }

  @Override
  public Document toDocument() {
    return document;
  }

  @Override
  public String toString() {
    return document.toJson();
  }
}
