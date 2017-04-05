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
package org.n52.proxy.connector.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.n52.proxy.connector.constellations.DatasetConstellation;
import static org.n52.proxy.connector.utils.EntityBuilder.createCategory;
import static org.n52.proxy.connector.utils.EntityBuilder.createFeature;
import static org.n52.proxy.connector.utils.EntityBuilder.createGeometry;
import static org.n52.proxy.connector.utils.EntityBuilder.createOffering;
import static org.n52.proxy.connector.utils.EntityBuilder.createPhenomenon;
import static org.n52.proxy.connector.utils.EntityBuilder.createProcedure;
import org.n52.proxy.db.beans.ProxyServiceEntity;
import org.n52.series.db.beans.CategoryEntity;
import org.n52.series.db.beans.FeatureEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.PhenomenonEntity;
import org.n52.series.db.beans.ProcedureEntity;

public class ServiceConstellation {

    // service
    private ProxyServiceEntity service;

    // map für procedures
    private final Map<String, ProcedureEntity> procedures = new HashMap<>();

    // map für offerings
    private final Map<String, OfferingEntity> offerings = new HashMap<>();

    // map für categories
    private final Map<String, CategoryEntity> categories = new HashMap<>();

    // map für phenomena
    private final Map<String, PhenomenonEntity> phenomena = new HashMap<>();

    // map für feature
    private final Map<String, FeatureEntity> features = new HashMap<>();

    // dataset collection
    private final Collection<DatasetConstellation> datasets = new HashSet<>();

    public ProxyServiceEntity getService() {
        return service;
    }

    public void setService(ProxyServiceEntity service) {
        this.service = service;
    }

    public Map<String, ProcedureEntity> getProcedures() {
        return procedures;
    }

    public boolean hasProcedure(String procedureId) {
        return procedures.containsKey(procedureId);
    }

    public Map<String, OfferingEntity> getOfferings() {
        return offerings;
    }

    public boolean hasOffering(String offeringId) {
        return offerings.containsKey(offeringId);
    }

    public Map<String, CategoryEntity> getCategories() {
        return categories;
    }

    public boolean hasCategories(String categoryId) {
        return categories.containsKey(categoryId);
    }

    public Map<String, PhenomenonEntity> getPhenomena() {
        return phenomena;
    }

    public boolean hasPhenomenon(String phenomenonId) {
        return phenomena.containsKey(phenomenonId);
    }

    public Map<String, FeatureEntity> getFeatures() {
        return features;
    }

    public boolean hasFeature(String featureId) {
        return features.containsKey(featureId);
    }

    public Collection<DatasetConstellation> getDatasets() {
        return datasets;
    }

    public CategoryEntity putCategory(String id, String name) {
        return categories.put(id, createCategory(id, name, service));
    }

    public FeatureEntity putFeature(String id, String name, double latitude, double longitude, int srid) {
        return features.put(id,
                createFeature(id,
                        name,
                        createGeometry(latitude, longitude, srid),
                        service
                )
        );
    }

    public OfferingEntity putOffering(String id, String name) {
        return offerings.put(id, createOffering(id, name, service));
    }

    public PhenomenonEntity putPhenomenon(String id, String name) {
        return phenomena.put(id, createPhenomenon(id, name, service));
    }

    public ProcedureEntity putProcedure(String id, String name, boolean insitu, boolean mobile) {
        return procedures.put(id, createProcedure(id, name, insitu, mobile, service));
    }

    public boolean add(DatasetConstellation e) {
        return datasets.add(e);
    }

}
