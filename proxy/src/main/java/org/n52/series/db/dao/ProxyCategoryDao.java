/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.n52.series.db.dao;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.Subqueries;
import org.n52.series.db.beans.CategoryEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.ServiceEntity;

/**
 *
 * @author jansch
 */
public class ProxyCategoryDao extends CategoryDao implements InsertDao<CategoryEntity>, ClearDao<CategoryEntity> {

//    private CategoryDao categoryDao;
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_SERVICE_PKID = "service.pkid";

    public ProxyCategoryDao(Session session) {
        super(session);
//        categoryDao = new CategoryDao(session);
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
                .add(Restrictions.eq(COLUMN_NAME, category.getName()))
                .add(Restrictions.eq(COLUMN_SERVICE_PKID, category.getService().getPkid()));
        return (CategoryEntity) criteria.uniqueResult();
    }

    @Override
    public void clearUnusedForService(ServiceEntity service) {
        Criteria criteria = session.createCriteria(getEntityClass())
                .add(Restrictions.eq("service.pkid", service.getPkid()))
                .add(Subqueries.propertyNotIn("pkid", createDetachedDatasetFilter()));
        criteria.list().forEach(entry -> {
            session.delete(entry);
        });
    }

    private DetachedCriteria createDetachedDatasetFilter() {
        DetachedCriteria filter = DetachedCriteria.forClass(DatasetEntity.class)
                .setProjection(Projections.distinct(Projections.property(getSeriesProperty())));
        return filter;
    }

}