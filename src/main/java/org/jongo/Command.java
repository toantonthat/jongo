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

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.jongo.marshall.Unmarshaller;
import org.jongo.query.Query;
import org.jongo.query.QueryFactory;

import java.util.ArrayList;
import java.util.List;

import static org.jongo.ResultHandlerFactory.newResultHandler;

public class Command {

  private final MongoDatabase db;
  private final Unmarshaller unmarshaller;
  private Query query;
  private boolean throwOnError;

  public Command(MongoDatabase db, Unmarshaller unmarshaller, QueryFactory queryFactory,
      String query, Object... parameters) {
    this.db = db;
    this.unmarshaller = unmarshaller;
    this.query = queryFactory.createQuery(query, parameters);
  }

  public Command throwOnError() {
    throwOnError = true;
    return this;
  }

  public ResultCommand field(String fieldName) {
    return new ResultCommand(fieldName);
  }

  public <T> T as(final Class<T> clazz) {
    return map(newResultHandler(clazz, unmarshaller));
  }

  public <T> T map(ResultHandler<T> resultHandler) {
    Document commandResult = executeCommand();
    return resultHandler.map(commandResult);
  }

  private Document executeCommand() {
    Document commandResult = db.runCommand(query.toDocument());
    if (throwOnError) {
      throw new RuntimeException("Command error: " + commandResult.getString("errmsg"));
    }
    return commandResult;
  }

  public class ResultCommand {

    private String fieldName;

    public ResultCommand(String fieldName) {
      this.fieldName = fieldName;
    }

    public <T> List<T> as(final Class<T> clazz) {
      return map(newResultHandler(clazz, unmarshaller));
    }

    public <T> List<T> map(ResultHandler<T> resultHandler) {
      Document commandResult = executeCommand();
      List<Document> results = (List<Document>) commandResult.get(fieldName);
      if (results == null) {
        return new ArrayList<T>();
      }
      List<T> mappedResult = new ArrayList<T>(results.size());
      for (Document document : results) {
        mappedResult.add(resultHandler.map(document));
      }
      return mappedResult;
    }
  }
}
