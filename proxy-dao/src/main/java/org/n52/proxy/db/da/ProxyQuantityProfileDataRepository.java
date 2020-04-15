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
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.n52.proxy.db.da;

import java.util.Map;

import org.hibernate.Session;
import org.n52.io.response.dataset.profile.ProfileData;
import org.n52.io.response.dataset.profile.ProfileValue;
import org.n52.proxy.connector.AbstractConnector;
import org.n52.proxy.db.beans.ProxyServiceEntity;
import org.n52.series.db.DataAccessException;
import org.n52.series.db.beans.DataEntity;
import org.n52.series.db.beans.ProfileDataEntity;
import org.n52.series.db.beans.ProfileDatasetEntity;
import org.n52.series.db.da.QuantityProfileDataRepository;
import org.n52.series.db.dao.DbQuery;

/**
 * @author Jan Schulte
 */
public class ProxyQuantityProfileDataRepository
        extends QuantityProfileDataRepository
        implements ProxyDataRepository<ProfileDatasetEntity, ProfileValue<Double>> {

    private Map<String, AbstractConnector> connectorMap;

    @Override
    public void setConnectorMap(Map connectorMap) {
        this.connectorMap = connectorMap;
    }

    private AbstractConnector getConnector(ProfileDatasetEntity profileDatasetEntity) {
        String connectorName = ((ProxyServiceEntity) profileDatasetEntity.getService()).getConnector();
        return this.connectorMap.get(connectorName);
    }

    @Override
    public ProfileValue getFirstValue(ProfileDatasetEntity profileDatasetEntity, Session session, DbQuery query) {
        DataEntity firstObs = getConnector(profileDatasetEntity).getFirstObservation(profileDatasetEntity).orElse(null);
        if (firstObs == null) return null;
        return createSeriesValueFor((ProfileDataEntity) firstObs, profileDatasetEntity, query);
    }

    @Override
    public ProfileValue getLastValue(ProfileDatasetEntity profileDatasetEntity, Session session, DbQuery query) {
        DataEntity lastObs = getConnector(profileDatasetEntity).getLastObservation(profileDatasetEntity).orElse(null);
        if (lastObs == null) return null;
        return createSeriesValueFor((ProfileDataEntity)lastObs, profileDatasetEntity, query);
    }

    @Override
    protected ProfileData assembleDataWithReferenceValues(ProfileDatasetEntity profileDatasetEntity, DbQuery dbQuery,
            Session session) throws DataAccessException {
        return assembleData(profileDatasetEntity, dbQuery, session);
    }

    @Override
    protected ProfileData assembleData(ProfileDatasetEntity profileDatasetEntity, DbQuery query, Session session) throws DataAccessException {
        ProfileData result = new ProfileData();
        this.getConnector(profileDatasetEntity)
                .getObservations(profileDatasetEntity, query)
                .stream()
                .map((entry) -> createSeriesValueFor((ProfileDataEntity) entry, profileDatasetEntity, query))
                .forEach(entry -> result.addNewValue(entry));
        return result;
    }

}
