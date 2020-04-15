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
package org.n52.proxy.connector.constellations;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

import org.n52.proxy.db.beans.ProxyServiceEntity;
import org.n52.series.db.beans.CategoryEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.FeatureEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.PhenomenonEntity;
import org.n52.series.db.beans.ProcedureEntity;

/**
 * @author Jan Schulte
 */
public abstract class DatasetConstellation<T extends DatasetEntity> {

    private final String procedure;
    private final String offering;
    private final String category;
    private final String phenomenon;
    private final String feature;

    private String domainId;

    public DatasetConstellation(String procedure, String offering, String category, String phenomenon, String feature) {
        this.procedure = procedure;
        this.offering = offering;
        this.category = category;
        this.phenomenon = phenomenon;
        this.feature = feature;
    }

    public String getProcedure() {
        return procedure;
    }

    public String getOffering() {
        return offering;
    }

    public String getCategory() {
        return category;
    }

    public String getPhenomenon() {
        return phenomenon;
    }

    public String getFeature() {
        return feature;
    }

    public String getDomainId() {
        return domainId;
    }

    public void setDomainId(String domainId) {
        this.domainId = domainId;
    }

    @Override
    public String toString() {
        return "DatasetConstellation{" + "procedure=" + procedure
                + ", offering=" + offering + ", category=" + category
                + ", phenomenon=" + phenomenon + ", feature=" + feature + '}';
    }

    public final DatasetEntity createDatasetEntity(
            ProcedureEntity procedure,
            CategoryEntity category,
            FeatureEntity feature,
            OfferingEntity offering,
            PhenomenonEntity phenomenon,
            ProxyServiceEntity service) {
        DatasetEntity datasetEntity = createDatasetEntity(service);
        datasetEntity.setDomainId(this.getDomainId());
        datasetEntity.setProcedure(procedure);
        datasetEntity.setCategory(category);
        datasetEntity.setFeature(feature);
        datasetEntity.setPhenomenon(phenomenon);
        datasetEntity.setOffering(offering);
        datasetEntity.setPublished(TRUE);
        datasetEntity.setDeleted(FALSE);
        datasetEntity.setService(service);
        return datasetEntity;
    };

    protected abstract T createDatasetEntity(ProxyServiceEntity service);

}
