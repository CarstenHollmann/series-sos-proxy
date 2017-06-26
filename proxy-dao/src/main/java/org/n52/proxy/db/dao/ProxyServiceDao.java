/*
 * Copyright (C) 2013-2017 52°North Initiative for Geospatial Open Source
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
package org.n52.proxy.db.dao;

import java.util.List;
import org.hibernate.Criteria;
import org.hibernate.Session;
import static org.hibernate.criterion.Restrictions.eq;

import org.n52.io.request.IoParameters;
import org.n52.proxy.db.beans.ProxyServiceEntity;
import org.n52.series.db.dao.DbQuery;
import org.n52.series.db.dao.ServiceDao;
import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;
import org.springframework.transaction.annotation.Transactional;

@Transactional
public class ProxyServiceDao extends ServiceDao implements InsertDao<ProxyServiceEntity> {

    private static final Logger LOGGER = getLogger(ProxyServiceDao.class);

    private static final String COLUMN_TYPE = "type";
    private static final String COLUMN_URL = "url";

    public ProxyServiceDao(Session session) {
        super(session);
    }

    @Override
    public ProxyServiceEntity getOrInsertInstance(ProxyServiceEntity service) {
        ProxyServiceEntity instance = getInstance(service);
        if (instance == null) {
            session.save(service);
            session.flush();
            session.refresh(service);
            LOGGER.debug("Save service: " + service);
            instance = service;
        }
        return instance;
    }

    private ProxyServiceEntity getInstance(ProxyServiceEntity service) {
        Criteria criteria = session.createCriteria(getProxyEntityClass())
                .add(eq(COLUMN_TYPE, service.getType()))
                .add(eq(COLUMN_URL, service.getUrl()));
        return (ProxyServiceEntity) criteria.uniqueResult();
    }

    protected Class<ProxyServiceEntity> getProxyEntityClass() {
        return ProxyServiceEntity.class;
    }
    public List<ProxyServiceEntity> getAllServices() {
        Criteria criteria = getDefaultCriteria(new DbQuery(IoParameters.createDefaults()));
        return criteria.list();
    }

    public void deleteInstance(ProxyServiceEntity service) {
        this.session.delete(service);
    }

}
