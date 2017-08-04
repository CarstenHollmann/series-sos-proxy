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

import static org.hibernate.criterion.DetachedCriteria.forClass;
import static org.hibernate.criterion.Projections.distinct;
import static org.hibernate.criterion.Projections.property;
import static org.hibernate.criterion.Restrictions.eq;
import static org.hibernate.criterion.Restrictions.in;
import static org.hibernate.criterion.Subqueries.propertyNotIn;
import static org.n52.series.db.beans.DescribableEntity.PROPERTY_DOMAIN_ID;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.ServiceEntity;
import org.n52.series.db.dao.OfferingDao;

import com.google.common.collect.Sets;

public class ProxyOfferingDao extends OfferingDao implements InsertDao<OfferingEntity>, ClearDao<OfferingEntity> {

    public ProxyOfferingDao(Session session) {
        super(session);
    }

    @Override
    public OfferingEntity getOrInsertInstance(OfferingEntity offering) {
        OfferingEntity instance = getInstance(offering);
        if (instance == null) {
            if (offering.hasParents()) {
                offering.setParents(insert(offering.getParents()));
            }
            if (offering.hasChildren()) {
                offering.setChildren(insert(offering.getChildren()));
            }
            session.save(offering);
            session.flush();
            session.refresh(offering);
            instance = offering;
        } else {
            updateOfferingTimestamps(instance, offering);
            session.update(instance);
            session.flush();
            session.refresh(instance);
        }
        return instance;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void clearUnusedForService(ServiceEntity service) {
        Criteria criteria = session.createCriteria(getEntityClass())
                .add(eq(COLUMN_SERVICE_PKID, service.getPkid()))
                .add(propertyNotIn("pkid", createDetachedDatasetFilter()));
        criteria.list().forEach(entry -> {
            session.delete(entry);
        });
    }

    private Set<OfferingEntity> insert(Set<OfferingEntity> set) {
        Set<OfferingEntity> inserted = Sets.newHashSet();
        for (OfferingEntity offeringEntity : set) {
            inserted.add(getOrInsertInstance(offeringEntity));
        }
        return inserted;
    }

    private OfferingEntity getInstance(OfferingEntity offering) {
        Criteria criteria = session.createCriteria(getEntityClass())
                .add(eq(PROPERTY_DOMAIN_ID, offering.getDomainId()))
                .add(eq(COLUMN_SERVICE_PKID, offering.getService().getPkid()));
        return (OfferingEntity) criteria.uniqueResult();
    }

    private DetachedCriteria createDetachedDatasetFilter() {
        DetachedCriteria filter = forClass(DatasetEntity.class)
                .setProjection(distinct(property(getDatasetProperty())));
        return filter;
    }

    private void updateOfferingTimestamps(OfferingEntity oldOffering, OfferingEntity newOffering) {
        if (oldOffering.getPhenomenonTimeStart() == null || (oldOffering.getPhenomenonTimeStart() != null
                && newOffering.getPhenomenonTimeStart() != null
                && oldOffering.getPhenomenonTimeStart().after(newOffering.getPhenomenonTimeStart()))) {
            oldOffering.setPhenomenonTimeStart(newOffering.getPhenomenonTimeStart());
        }
        if (oldOffering.getPhenomenonTimeEnd() == null || (oldOffering.getPhenomenonTimeEnd() != null
                && newOffering.getPhenomenonTimeEnd() != null
                && oldOffering.getPhenomenonTimeEnd().before(newOffering.getPhenomenonTimeEnd()))) {
            oldOffering.setPhenomenonTimeEnd(newOffering.getPhenomenonTimeEnd());
        }
        if (oldOffering.getResultTimeStart() == null || (oldOffering.getResultTimeStart() != null
                && newOffering.getResultTimeStart() != null
                && oldOffering.getResultTimeStart().after(newOffering.getResultTimeStart()))) {
            oldOffering.setResultTimeStart(newOffering.getResultTimeStart());
        }
        if (oldOffering.getResultTimeEnd() == null || (oldOffering.getResultTimeEnd() != null
                && newOffering.getResultTimeEnd() != null
                && oldOffering.getResultTimeEnd().before(newOffering.getResultTimeEnd()))) {
            oldOffering.setResultTimeEnd(newOffering.getResultTimeEnd());
        }
        session.saveOrUpdate(oldOffering);
        session.flush();
    }

    @SuppressWarnings("unchecked")
    public List<OfferingEntity> getInstancesFor(Collection<String> domainIds) {
        Criteria c = getDefaultCriteria(ProxyDbQuery.createDefaults())
                .add(in(PROPERTY_DOMAIN_ID, domainIds));
        return c.list();
    }
}
