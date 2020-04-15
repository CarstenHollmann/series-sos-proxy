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
package org.n52.proxy.db.dao;

import static org.hibernate.criterion.DetachedCriteria.forClass;
import static org.hibernate.criterion.Projections.distinct;
import static org.hibernate.criterion.Projections.property;
import static org.hibernate.criterion.Restrictions.eq;
import static org.hibernate.criterion.Subqueries.propertyNotIn;
import static org.n52.series.db.beans.DescribableEntity.PROPERTY_DOMAIN_ID;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;
import org.n52.series.db.beans.CategoryEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.ServiceEntity;
import org.n52.series.db.dao.CategoryDao;

public class ProxyCategoryDao extends CategoryDao implements InsertDao<CategoryEntity>, ClearDao<CategoryEntity> {

    public ProxyCategoryDao(Session session) {
        super(session);
    }

    @Override
    public CategoryEntity getOrInsertInstance(CategoryEntity category) {
        CategoryEntity instance = getInstance(category);
        if (instance == null) {
            this.session.save(category);
            instance = category;
        }
        return instance;
    }

    private CategoryEntity getInstance(CategoryEntity category) {
        Criteria criteria = session.createCriteria(getEntityClass())
                .add(eq(PROPERTY_DOMAIN_ID, category.getDomainId()))
                .add(eq(COLUMN_SERVICE_PKID, category.getService().getPkid()));
        return (CategoryEntity) criteria.uniqueResult();
    }

    @Override
    public void clearUnusedForService(ServiceEntity service) {
        Criteria criteria = session.createCriteria(getEntityClass())
                .add(eq(COLUMN_SERVICE_PKID, service.getPkid()))
                .add(propertyNotIn("pkid", createDetachedDatasetFilter()));
        criteria.list().forEach(entry -> {
            session.delete(entry);
        });
    }

    private DetachedCriteria createDetachedDatasetFilter() {
        DetachedCriteria filter = forClass(DatasetEntity.class)
                .setProjection(distinct(property(getDatasetProperty())));
        return filter;
    }

}
