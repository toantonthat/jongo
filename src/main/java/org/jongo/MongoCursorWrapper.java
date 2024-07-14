package org.jongo;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import java.util.function.Consumer;
import org.bson.Document;

import java.util.Iterator;

public class MongoCursorWrapper<T> implements MongoCursor<T> {

  private final MongoCursor<Document> cursor;
  private final ResultHandler<T> resultHandler;

  public MongoCursorWrapper(MongoCursor<Document> cursor, ResultHandler<T> resultHandler) {
    this.cursor = cursor;
    this.resultHandler = resultHandler;
  }

  @Override
  public void close() {
    cursor.close();
  }

  @Override
  public boolean hasNext() {
    return cursor.hasNext();
  }

  @Override
  public T next() {
    Document document = cursor.next();
    return resultHandler.map(document);
  }

  @Override
  public int available() {
    return 0;
  }

  @Override
  public T tryNext() {
    Document document = cursor.tryNext();
    return document != null ? resultHandler.map(document) : null;
  }

  @Override
  public ServerCursor getServerCursor() {
    return cursor.getServerCursor();
  }

  @Override
  public ServerAddress getServerAddress() {
    return cursor.getServerAddress();
  }

  @Override
  public void forEachRemaining(Consumer<? super T> action) {
    MongoCursor.super.forEachRemaining(action);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove operation is not supported");
  }
}
