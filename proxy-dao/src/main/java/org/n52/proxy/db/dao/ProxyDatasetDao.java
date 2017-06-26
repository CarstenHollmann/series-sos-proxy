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

import static java.util.stream.Collectors.toSet;
import static org.hibernate.criterion.Restrictions.eq;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.List;
import java.util.Set;

import org.hibernate.Criteria;
import org.hibernate.Session;

import org.n52.io.request.IoParameters;
import org.n52.proxy.connector.constellations.QuantityDatasetConstellation;
import org.n52.proxy.db.beans.ProxyServiceEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.QuantityDatasetEntity;
import org.n52.series.db.beans.ServiceEntity;
import org.n52.series.db.beans.UnitEntity;
import org.n52.series.db.dao.DatasetDao;
import org.n52.series.db.dao.DbQuery;
import org.slf4j.Logger;

public class ProxyDatasetDao<T extends DatasetEntity> extends DatasetDao<T> implements InsertDao<T> {

    private static final Logger LOGGER = getLogger(ProxyDatasetDao.class);

    private static final String COLUMN_VALUETYPE = "valueType";
    private static final String COLUMN_CATEGORY_PKID = "category.pkid";
    private static final String COLUMN_FEATURE_PKID = "feature.pkid";
    private static final String COLUMN_PROCEDURE_PKID = "procedure.pkid";
    private static final String COLUMN_PHENOMENON_PKID = "phenomenon.pkid";
    private static final String COLUMN_OFFERING_PKID = "offering.pkid";

    public ProxyDatasetDao(Session session) {
        super(session);
    }

    public ProxyDatasetDao(Session session, Class<T> clazz) {
        super(session, clazz);
    }

    @Override
    public T getOrInsertInstance(T dataset) {
        if (dataset.getUnit() != null) {
            dataset.setUnit(getOrInsertUnit(dataset.getUnit()));
        }
        DatasetEntity instance = getInstance(dataset);
        if (instance == null) {
            session.save(dataset);
            LOGGER.debug("Save dataset: " + dataset);
        } else {
            // TODO find good solution to recreate the dataset entities
            instance.setDomainId(null);
//            instance.setDeleted(Boolean.FALSE);
            instance.setPublished(Boolean.TRUE);
            updateSeriesWithFirstLatestValues(instance, dataset);
            session.update(instance);
            dataset = (T)instance;
            LOGGER.debug("Mark dataset as undeleted: " + instance);
        }
        return (T) instance;
    }

    public void updateSeriesWithFirstLatestValues(DatasetEntity oldDataSet, T newDataSet) {
        boolean minChanged = false;
        boolean maxChanged = false;
        if (oldDataSet.getFirstValueAt() == null ||
                 (oldDataSet.getFirstValueAt() != null && oldDataSet.getFirstValueAt().after(
                         newDataSet.getFirstValueAt()))) {
            minChanged = true;
            oldDataSet.setFirstValueAt(newDataSet.getFirstValueAt());
        }
        if (oldDataSet.getLastValueAt() == null ||
                (oldDataSet.getLastValueAt() != null && oldDataSet.getLastValueAt().before(
                        newDataSet.getLastValueAt()))) {
            maxChanged = true;
            oldDataSet.setLastValueAt(newDataSet.getLastValueAt());
        }

        if (newDataSet instanceof QuantityDatasetEntity) {
            if (minChanged) {
                oldDataSet.setFirstValue(((QuantityDatasetEntity) newDataSet).getFirstValue());
            }
            if (maxChanged) {
                oldDataSet.setLastValue(((QuantityDatasetEntity) newDataSet).getLastValue());
            }
            if (oldDataSet.getUnit() == null && newDataSet.getUnit() != null) {
                // TODO check if both unit are equal. If not throw exception?
                oldDataSet.setUnit(newDataSet.getUnit());
            }
        }
        session.saveOrUpdate(oldDataSet);
        session.flush();
    }

    public UnitEntity getOrInsertUnit(UnitEntity unit) {
        UnitEntity instance = getUnit(unit);
        if (instance == null) {
            session.save(unit);
            instance = unit;
        }
        return instance;
    }

    public Set<Long> getIdsForService(ProxyServiceEntity service) {
        List<T> datasets = getDatasetsForService(service);
        return datasets
                .stream()
                .map((dataset) -> {
                    return dataset.getPkid();
                })
                .collect(toSet());
    }

    public void removeDatasets(Set<Long> datasetIds) {
        datasetIds.forEach((id) -> {
            session.delete(session.get(DatasetEntity.class, id));
        });
        session.flush();
    }

    public void removeAllOfService(ProxyServiceEntity service) {
        getDefaultCriteria(ProxyDbQuery.createDefaults())
                .add(eq(COLUMN_SERVICE_PKID, service.getPkid()))
                .list()
                .forEach((dataset) -> session.delete(dataset));
        session.createCriteria(UnitEntity.class)
                .add(eq(COLUMN_SERVICE_PKID, service.getPkid()))
                .list()
                .forEach((unit) -> session.delete(unit));
        session.flush();
    }

    private UnitEntity getUnit(UnitEntity unit) {
        Criteria criteria = session.createCriteria(UnitEntity.class)
                .add(eq("name", unit.getName()))
                .add(eq(COLUMN_SERVICE_PKID, unit.getService().getPkid()));
        return (UnitEntity) criteria.uniqueResult();
    }

    private DatasetEntity getInstance(DatasetEntity dataset) {
        Criteria criteria = getDefaultCriteria(ProxyDbQuery.createDefaults())
                .add(eq(COLUMN_VALUETYPE, dataset.getValueType()))
                .add(eq(COLUMN_CATEGORY_PKID, dataset.getCategory().getPkid()))
                .add(eq(COLUMN_FEATURE_PKID, dataset.getFeature().getPkid()))
                .add(eq(COLUMN_PROCEDURE_PKID, dataset.getProcedure().getPkid()))
                .add(eq(COLUMN_PHENOMENON_PKID, dataset.getPhenomenon().getPkid()))
                .add(eq(COLUMN_OFFERING_PKID, dataset.getOffering().getPkid()))
                .add(eq(COLUMN_SERVICE_PKID, dataset.getService().getPkid()));
        return (T) criteria.uniqueResult();
    }

    private List<T> getDatasetsForService(ServiceEntity service) {
        Criteria criteria = getDefaultCriteria(ProxyDbQuery.createDefaults())
                .add(eq(COLUMN_SERVICE_PKID, service.getPkid()));
        return criteria.list();
    }

}
