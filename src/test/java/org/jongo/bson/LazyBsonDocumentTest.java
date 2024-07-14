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
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LazyBsonDocumentTest {

    @Test
    public void shouldUseBsonSize() throws Exception {
        LazyBsonDocument document = new LazyBsonDocument(new byte[]{6, 0, 0, 0, 0});

        assertThat(document.getSize()).isEqualTo(6);
    }

    @Test
    public void testByte() {
        //byte[] bytes1 = new byte[]{37, 0, 0, 0, 2, 110, 97, 109, 101, 0, 5, 0, 0, 0, 74, 111, 104, 110, 0, 7, 95, 105, 100, 0, 102, -110, 116, -60, 57, -71, -51, 5, 54, 2, -91, 78, 0};
        byte[] bytes2 = new byte[]{37, 0, 0, 0, 2, 110, 97, 109, 101, 0, 5, 0, 0, 0, 74, 111, 104, 110, 0, 7, 95, 105, 100, 0, 102, -110, 118, 56, -81, -103, 112, 108, 74, 101, 69, -104, 0};
        byte[] bytes3 = new byte[]{6, 0, 0, 0, 0};
//        RawBsonDocument rawDoc = new RawBsonDocument(bytes2);
//        Document doc = Document.parse(rawDoc.toJson());

        LazyBsonDocument2 document = new LazyBsonDocument2(bytes3);
        Assert.assertNotNull(document);
//        assertThat(document.getSize()).isEqualTo(6);
    }
}
