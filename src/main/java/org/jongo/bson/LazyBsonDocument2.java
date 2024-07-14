package org.jongo.bson;


import org.bson.Document;
import org.bson.RawBsonDocument;

public class LazyBsonDocument2 implements BsonDocument {
  private final byte[] bytes;

  LazyBsonDocument2(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public byte[] toByteArray() {
    return new byte[0];
  }

  @Override
  public Document toDocument() {
//    RawBsonDocument rawDoc = new RawBsonDocument(bytes);
//    Document.parse(rawDoc.toJson())
    RawBsonDocument rawDoc = new RawBsonDocument(bytes);
    return Document.parse(rawDoc.toJson());
  }

  @Override
  public int getSize() {
    return 0;
  }

  @Override
  public String toString() {
    return toDocument().toJson();
  }
}
