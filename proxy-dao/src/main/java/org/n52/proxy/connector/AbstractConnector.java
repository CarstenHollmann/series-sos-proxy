/*
 * Copyright (C) 2015-2020 52°North Initiative for Geospatial Open Source
 * Software GmbH
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * If the program is linked with libraries which are licensed under one of
 * the following licenses, the combination of the program with the linked
 * library is not considered a "derivative work" of the program:
 *
 *     - Apache License, version 2.0
 *     - Apache Software License, version 1.0
 *     - GNU Lesser General Public License, version 3
 *     - Mozilla Public License, versions 1.0, 1.1 and 2.0
 *     - Common Development and Distribution License (CDDL), version 1.0
 *
 * Therefore the distribution of the program linked with libraries licensed
 * under the aforementioned licenses, is permitted by the copyright holders
 * if the distribution is compliant with both the GNU General Public License
 * version 2 and the aforementioned licenses.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details.
 */
package org.n52.proxy.connector;

import java.util.List;
import java.util.Optional;

import org.apache.http.HttpResponse;
import org.apache.xmlbeans.XmlObject;
import org.n52.proxy.web.SimpleHttpClient;
import org.n52.series.db.beans.DataEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.UnitEntity;
import org.n52.series.db.dao.DbQuery;
import org.n52.svalbard.decode.DecoderRepository;
import org.n52.svalbard.encode.EncoderRepository;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author Jan Schulte
 */
public abstract class AbstractConnector {

    @Autowired
    protected DecoderRepository decoderRepository;

    @Autowired
    protected EncoderRepository encoderRepository;

    private static final int CONNECTION_TIMEOUT = 30000;

    public String getConnectorName() {
        return getClass().getName();
    }

    protected HttpResponse sendGetRequest(String uri) {
        return new SimpleHttpClient(CONNECTION_TIMEOUT, CONNECTION_TIMEOUT).executeGet(uri);
    }

    protected HttpResponse sendPostRequest(XmlObject request, String uri) {
        return new SimpleHttpClient(CONNECTION_TIMEOUT, CONNECTION_TIMEOUT).executePost(uri, request);
    }

    public abstract List<DataEntity> getObservations(DatasetEntity seriesEntity, DbQuery query);

    public abstract UnitEntity getUom(DatasetEntity seriesEntity);

    public abstract Optional<DataEntity> getFirstObservation(DatasetEntity entity);

    public abstract Optional<DataEntity> getLastObservation(DatasetEntity entity);
}
