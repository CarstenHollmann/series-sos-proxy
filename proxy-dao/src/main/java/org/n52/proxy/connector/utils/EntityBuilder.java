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

import static org.n52.shetland.util.JTSHelper.createGeometryFromWKT;
import static org.slf4j.LoggerFactory.getLogger;

import org.locationtech.jts.io.ParseException;
import org.n52.proxy.db.beans.ProxyServiceEntity;
import org.n52.proxy.decode.JTSConverter;
import org.n52.series.db.beans.CategoryEntity;
import org.n52.series.db.beans.FeatureEntity;
import org.n52.series.db.beans.GeometryEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.PhenomenonEntity;
import org.n52.series.db.beans.ProcedureEntity;
import org.n52.series.db.beans.UnitEntity;
import org.n52.shetland.ogc.ows.exception.CodedException;


public class EntityBuilder {

    private static final org.slf4j.Logger LOGGER = getLogger(EntityBuilder.class);

    private EntityBuilder() {
    }

    public static ProxyServiceEntity createService(String name, String description, String connector, String url,
            String version) {
        ProxyServiceEntity service = new ProxyServiceEntity();
        service.setName(name);
        service.setDescription(description);
        service.setVersion(version);
        service.setType("SOS");
        service.setUrl(url);
        service.setConnector(connector);
        return service;
    }

    public static ProcedureEntity createProcedure(String domainId, String name, boolean insitu, boolean mobile,
            ProxyServiceEntity service) {
        ProcedureEntity procedure = new ProcedureEntity();
        procedure.setName(name);
        procedure.setDomainId(domainId);
        procedure.setInsitu(insitu);
        procedure.setMobile(mobile);
        procedure.setService(service);
        return procedure;
    }

    public static OfferingEntity createOffering(String domainId, String name, ProxyServiceEntity service) {
        OfferingEntity offering = new OfferingEntity();
        offering.setDomainId(domainId);
        offering.setName(name);
        offering.setService(service);
        return offering;
    }

    public static CategoryEntity createCategory(String domainId, String name, ProxyServiceEntity service) {
        CategoryEntity category = new CategoryEntity();
        category.setName(name);
        category.setDomainId(domainId);
        category.setService(service);
        return category;
    }

    public static FeatureEntity createFeature(String domainId, String name, String description, GeometryEntity geometry,
            ProxyServiceEntity service) {
        FeatureEntity feature = new FeatureEntity();
        feature.setName(name);
        feature.setDescription(description);
        feature.setDomainId(domainId);
        feature.setGeometryEntity(geometry);
        feature.setService(service);
        return feature;
    }

    public static GeometryEntity createGeometry(double latitude, double longitude, int srid) {
        GeometryEntity geometry = new GeometryEntity();
        try {
            geometry.setGeometry(JTSConverter.convert(createGeometryFromWKT("POINT (" + longitude + " " + latitude + ")", srid)));
        } catch (CodedException | ParseException ex) {
            LOGGER.error(ex.getLocalizedMessage(), ex);
        }
        return geometry;
    }

    public static PhenomenonEntity createPhenomenon(String domainId, String name, ProxyServiceEntity service) {
        PhenomenonEntity phenomenon = new PhenomenonEntity();
        phenomenon.setName(name);
        phenomenon.setDomainId(domainId);
        phenomenon.setService(service);
        return phenomenon;
    }

    public static UnitEntity createUnit(String unit, String unitDescription, ProxyServiceEntity service) {
        UnitEntity entity = new UnitEntity();
        entity.setName(unit);
        entity.setDescription(unitDescription);
        entity.setService(service);
        return entity;
    }

}
