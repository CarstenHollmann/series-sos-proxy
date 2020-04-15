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

import static org.n52.proxy.connector.utils.EntityBuilder.createUnit;

import java.util.Date;

import org.n52.proxy.db.beans.ProxyServiceEntity;
import org.n52.series.db.beans.QuantityDatasetEntity;
import org.n52.series.db.beans.UnitEntity;

/**
 * @author Jan Schulte
 */
public class QuantityDatasetConstellation extends DatasetConstellation<QuantityDatasetEntity> {

    private UnitEntity unit;

    public QuantityDatasetConstellation(String procedure, String offering, String category, String phenomenon,
            String feature) {
        super(procedure, offering, category, phenomenon, feature);
    }

    public UnitEntity getUnit() {
        return unit;
    }

    public void setUnit(UnitEntity unit) {
        this.unit = unit;
    }

    @Override
    protected QuantityDatasetEntity createDatasetEntity(ProxyServiceEntity service) {
        QuantityDatasetEntity quantityDataset = new QuantityDatasetEntity();
        // add empty unit entity, will be replaced later in the repositories
        if (unit == null) {
            // create empty unit
            unit = createUnit("", null, service);
        }
        quantityDataset.setUnit(unit);
        quantityDataset.setFirstValueAt(new Date());
        quantityDataset.setLastValueAt(new Date());
        return quantityDataset;
    }

}
