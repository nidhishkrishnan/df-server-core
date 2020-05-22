/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cloud.dataflow.server.batch;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.repository.dao.JdbcJobExecutionDao;
import org.springframework.batch.core.repository.dao.JdbcJobInstanceDao;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.incrementer.AbstractDataFieldMaxValueIncrementer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Dave Syer
 */
public class JdbcSearchableJobInstanceDao extends JdbcJobInstanceDao implements SearchableJobInstanceDao {

    private static final String GET_COUNT_BY_JOB_NAME = "SELECT COUNT(1) from %PREFIX%JOB_INSTANCE "
            + "where JOB_NAME=?";

    /**
     * @see JdbcJobExecutionDao#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {

        setJobIncrementer(new AbstractDataFieldMaxValueIncrementer() {
            @Override
            protected long getNextKey() {
                return 0;
            }
        });

        super.afterPropertiesSet();

    }

    /**
     * @see SearchableJobInstanceDao#countJobInstances (String)
     */
    public int countJobInstances(String name) {
        return getJdbcTemplate().queryForObject(getQuery(GET_COUNT_BY_JOB_NAME), Integer.class, name);
    }

    @Override
    public Collection<JobInstance> getJobInstances(String jobName, String jobState, int start, int count) {
        ResultSetExtractor<List<JobInstance>> extractor = new ResultSetExtractor<List<JobInstance>>() {
            private List<JobInstance> list = new ArrayList();

            public List<JobInstance> extractData(ResultSet rs) throws SQLException, DataAccessException {
                int rowNum;
                for (rowNum = 0; rowNum < start && rs.next(); ++rowNum) {
                }

                while (rowNum < start + count && rs.next()) {
                    RowMapper<JobInstance> rowMapper = JdbcSearchableJobInstanceDao.this.new JobInstanceRowMapper();
                    this.list.add(rowMapper.mapRow(rs, rowNum));
                    ++rowNum;
                }

                return this.list;
            }
        };
        StringBuilder queryBuilder = new StringBuilder();
        List params = new ArrayList();

        queryBuilder.append("SELECT inst.JOB_INSTANCE_ID, inst.JOB_NAME from %PREFIX%JOB_EXECUTION exe JOIN BATCH_JOB_INSTANCE inst ON inst.JOB_INSTANCE_ID = exe.JOB_INSTANCE_ID WHERE TRUE ");
        //stringBuilder.append("SELECT JOB_INSTANCE_ID, JOB_NAME from %PREFIX%JOB_INSTANCE WHERE 1 ");
        if (StringUtils.isNotBlank(jobName)) {
            queryBuilder.append("AND inst.JOB_NAME = ? ");
            params.add(jobName);
        }
        if (StringUtils.isNotBlank(jobState)) {
            queryBuilder.append("AND exe.STATUS = ? ");
            params.add(jobState);
        }

        queryBuilder.append("GROUP BY inst.JOB_INSTANCE_ID ORDER BY inst.JOB_INSTANCE_ID DESC");

        List<JobInstance> result = getJdbcTemplate().query(this.getQuery(queryBuilder.toString()), params.toArray(), extractor);
        return result;
    }

    private final class JobInstanceRowMapper implements RowMapper<JobInstance> {
        public JobInstanceRowMapper() {
        }

        public JobInstance mapRow(ResultSet rs, int rowNum) throws SQLException {
            JobInstance jobInstance = new JobInstance(rs.getLong(1), rs.getString(2));
            jobInstance.incrementVersion();
            return jobInstance;
        }
    }

}
