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
package org.n52.proxy.connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.n52.proxy.config.DataSourceConfiguration;
import org.n52.proxy.connector.constellations.MeasurementDatasetConstellation;
import org.n52.proxy.connector.utils.ConnectorHelper;
import org.n52.proxy.connector.utils.ServiceConstellation;
import org.n52.series.db.beans.DataEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.MeasurementDataEntity;
import org.n52.series.db.beans.UnitEntity;
import org.n52.series.db.dao.DbQuery;
import org.n52.shetland.ogc.filter.FilterConstants;
import org.n52.shetland.ogc.filter.TemporalFilter;
import org.n52.shetland.ogc.gml.AbstractFeature;
import org.n52.shetland.ogc.gml.time.Time;
import org.n52.shetland.ogc.gml.time.TimeInstant;
import org.n52.shetland.ogc.gml.time.TimePeriod;
import org.n52.shetland.ogc.om.SingleObservationValue;
import org.n52.shetland.ogc.om.features.FeatureCollection;
import org.n52.shetland.ogc.om.features.samplingFeatures.SamplingFeature;
import org.n52.shetland.ogc.om.values.QuantityValue;
import org.n52.shetland.ogc.ows.service.GetCapabilitiesResponse;
import org.n52.shetland.ogc.sos.Sos2Constants;
import org.n52.shetland.ogc.sos.SosCapabilities;
import org.n52.shetland.ogc.sos.SosConstants;
import org.n52.shetland.ogc.sos.SosObservationOffering;
import org.n52.shetland.ogc.sos.request.GetFeatureOfInterestRequest;
import org.n52.shetland.ogc.sos.request.GetObservationRequest;
import org.n52.shetland.ogc.sos.response.GetFeatureOfInterestResponse;
import org.n52.shetland.ogc.sos.response.GetObservationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jan Schulte
 */
public class HydroSOSConnector extends AbstractSosConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(HydroSOSConnector.class);

    @Override
    protected boolean canHandle(DataSourceConfiguration config, GetCapabilitiesResponse capabilities) {
        return false;
    }

    @Override
    public ServiceConstellation getConstellation(DataSourceConfiguration config, GetCapabilitiesResponse capabilities) {
        ServiceConstellation serviceConstellation = new ServiceConstellation();
        config.setVersion(Sos2Constants.SERVICEVERSION);
        config.setConnector(getConnectorName());
        ConnectorHelper.addService(config, serviceConstellation);
        SosCapabilities sosCaps = (SosCapabilities) capabilities.getCapabilities();
        addDatasets(serviceConstellation, sosCaps, config.getUrl());
        return serviceConstellation;
    }

    @Override
    public List<DataEntity> getObservations(DatasetEntity seriesEntity, DbQuery query) {
        GetObservationResponse obsResp = createObservationResponse(seriesEntity, query);

        List<DataEntity> data = new ArrayList<>();

        obsResp.getObservationCollection().forEach((observation) -> {
            MeasurementDataEntity entity = new MeasurementDataEntity();
            SingleObservationValue obsValue = (SingleObservationValue) observation.getValue();

            TimeInstant instant = (TimeInstant) obsValue.getPhenomenonTime();
            entity.setTimestart(instant.getValue().toDate());
            entity.setTimeend(instant.getValue().toDate());
            QuantityValue value = (QuantityValue) obsValue.getValue();
            entity.setValue(value.getValue());

            data.add(entity);
        });
        LOGGER.info("Found " + data.size() + " Entries");
        return data;
    }

    @Override
    public UnitEntity getUom(DatasetEntity seriesEntity) {
        // TODO implement
        throw new UnsupportedOperationException("getUom not supported yet.");
    }

    @Override
    public Optional<DataEntity> getFirstObservation(DatasetEntity entity) {
        // TODO implement
        throw new UnsupportedOperationException("getFirstObservation not supported yet.");
    }

    @Override
    public Optional<DataEntity> getLastObservation(DatasetEntity entity) {
        // TODO implement
        throw new UnsupportedOperationException("getLastObservation not supported yet.");
    }

    private void addDatasets(ServiceConstellation serviceConstellation, SosCapabilities sosCaps, String url) {
        if (sosCaps != null) {
            sosCaps.getContents().ifPresent((obsOffs) -> {
                obsOffs.forEach((obsOff) -> {
                    addByOffering(obsOff, serviceConstellation, url);
                });
            });
        }
    }

    private void addByOffering(SosObservationOffering obsOff, ServiceConstellation serviceConstellation, String url) {
        String offeringId = ConnectorHelper.addOffering(obsOff, serviceConstellation);

        obsOff.getProcedures().forEach((procedureId) -> {
            ConnectorHelper.addProcedure(procedureId, true, false, serviceConstellation);
            obsOff.getObservableProperties().forEach(phenomenonId -> {
                ConnectorHelper.addPhenomenon(phenomenonId, serviceConstellation);
                String categoryId = ConnectorHelper.addCategory(phenomenonId, serviceConstellation);

                GetFeatureOfInterestResponse foiResponse = getFeatureOfInterestResponse(procedureId, url);
                AbstractFeature abstractFeature = foiResponse.getAbstractFeature();
                if (abstractFeature instanceof FeatureCollection) {
                    FeatureCollection featureCollection = (FeatureCollection) abstractFeature;
                    featureCollection.getMembers().forEach((key, feature) -> {
                        String featureId = ConnectorHelper.addFeature((SamplingFeature) feature, serviceConstellation);
                        // TODO maybe not only MeasurementDatasetConstellation
                        serviceConstellation.add(
                                new MeasurementDatasetConstellation(procedureId, offeringId, categoryId,
                                        phenomenonId, featureId));
                    });
                }
            });
        });
    }

    private GetFeatureOfInterestResponse getFeatureOfInterestResponse(String procedureId, String url) {
        GetFeatureOfInterestRequest request = new GetFeatureOfInterestRequest(SosConstants.SOS,
                Sos2Constants.SERVICEVERSION);
        request.setProcedures(new ArrayList<>(Arrays.asList(procedureId)));
        return (GetFeatureOfInterestResponse) getSosResponseFor(request, Sos2Constants.NS_SOS_20, url);
    }

    private GetObservationResponse createObservationResponse(DatasetEntity seriesEntity, DbQuery query) {
        GetObservationRequest request = new GetObservationRequest(SosConstants.SOS, Sos2Constants.SERVICEVERSION);
        request.setProcedures(new ArrayList<>(Arrays.asList(seriesEntity.getProcedure().getDomainId())));
        request.setOfferings(new ArrayList<>(Arrays.asList(seriesEntity.getOffering().getDomainId())));
        request.setObservedProperties(new ArrayList<>(Arrays.asList(seriesEntity.getPhenomenon().getDomainId())));
        request.setFeatureIdentifiers(new ArrayList<>(Arrays.asList(seriesEntity.getFeature().getDomainId())));
        Time time = new TimePeriod(query.getTimespan().getStart(), query.getTimespan().getEnd());
        TemporalFilter temporalFilter = new TemporalFilter(FilterConstants.TimeOperator.TM_During, time,
                "phenomenonTime");
        request.setTemporalFilters(new ArrayList<>(Arrays.asList(temporalFilter)));
        return (GetObservationResponse) this.getSosResponseFor(request, Sos2Constants.NS_SOS_20,
                seriesEntity.getService().getUrl());
    }
}
