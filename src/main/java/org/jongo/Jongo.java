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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.jongo.bson.BsonDBDecoder;
import org.jongo.bson.BsonDBEncoder;
import org.jongo.query.Query;

import static org.jongo.marshall.jackson.JacksonMapper.Builder.jacksonMapper;

public class Jongo {

    private final MongoDatabase database;
    private final Mapper mapper;

    public Jongo(MongoDatabase database) {
        this(database, jacksonMapper().build());
    }

    public Jongo(MongoDatabase database, Mapper mapper) {
        this.database = database;
        this.mapper = mapper;
    }

    public MongoCollectionWrapper getCollection(String name) {
        MongoCollection<Document> dbCollection = database.getCollection(name);
//        dbCollection.setDBDecoderFactory(BsonDBDecoder.FACTORY);
//        dbCollection.setDBEncoderFactory(BsonDBEncoder.FACTORY);
//        dbCollection.setReadConcern(database.getReadConcern());
        return new MongoCollectionWrapper(dbCollection, mapper);
    }

    public MongoDatabase getDatabase() {
        return database;
    }

    public Mapper getMapper() {
        return mapper;
    }

    public Query createQuery(String query, Object... parameters) {
        return mapper.getQueryFactory().createQuery(query, parameters);
    }

    public Command runCommand(String query) {
        return runCommand(query, new Object[0]);
    }

    public Command runCommand(String query, Object... parameters) {
        return new Command(database, mapper.getUnmarshaller(), mapper.getQueryFactory(), query, parameters);
    }
}
