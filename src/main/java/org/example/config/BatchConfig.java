package org.example.config;

import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.dao.DefaultExecutionContextSerializer;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.*;
import org.springframework.batch.item.database.support.DataFieldMaxValueIncrementerFactory;
import org.springframework.batch.item.database.support.DefaultDataFieldMaxValueIncrementerFactory;
import org.springframework.batch.support.DatabaseType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.jdbc.support.incrementer.MySQLMaxValueIncrementer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.*;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.UUID;

@Configuration
public class BatchConfig {


    @Bean
    public PlatformTransactionManager transactionManager()
    {
        PlatformTransactionManager transactionManager=new DataSourceTransactionManager(dataSource());
        return transactionManager;
    }

        @Bean
        public DataSource dataSource()
        {
            DriverManagerDataSource ds=new DriverManagerDataSource("jdbc:mysql://localhost:3306/batch_db","root","billy123");
            return ds;

        }

        @Bean
        public Job printJob() throws Exception {
            Job job=new JobBuilder("job builder",jobRepository()).incrementer(new RunIdIncrementer()).listener(new JobExecutionListener(){
                @Override
                public void beforeJob(JobExecution jobExecution) {
                    System.out.println("Before execution");
                }

                @Override
                public void afterJob(JobExecution jobExecution) {
                    System.out.println("after execution");
                }
            }).flow(getStep1()).end().build();

            return job;

        }

        @Bean
        public Step getStep1() throws Exception {
            Step step= new StepBuilder("step 1",jobRepository()).<Data,Data>chunk(1,transactionManager()).reader(new ItemReader<Data>() {
                @Override
                public Data read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                    return new Data(UUID.randomUUID().toString());
                }
            }).processor(new DataProcessor()).writer(new DataWriter()).taskExecutor(taskExecutor()).build();
            return step;
        }

        @Bean
        public JobRepository jobRepository() throws Exception {

                JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
                factory.setDataSource(dataSource());
                factory.setDatabaseType(DatabaseType.MYSQL.getProductName());
                factory.setTransactionManager(transactionManager());
                factory.setIsolationLevelForCreate("ISOLATION_SERIALIZABLE");
                factory.setTablePrefix("BATCH_");
                factory.setMaxVarCharLength(1200);
                factory.setIncrementerFactory(new DefaultDataFieldMaxValueIncrementerFactory(dataSource()));
                factory.setJobKeyGenerator(new DefaultJobKeyGenerator());
                factory.setJdbcOperations(new JdbcTemplate(dataSource()));
                factory.setConversionService(new DefaultConversionService());
                factory.setSerializer(new DefaultExecutionContextSerializer());
                return factory.getObject();
        }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }

    @Bean
    public JobLauncher jobLauncher() throws Exception {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }



}

class Data
{
    private String value;

    public Data(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

class DataProcessor implements ItemProcessor<Data, Data> {

    @Override
    public Data process(Data item) throws Exception {
        return new Data(item.getValue().toUpperCase());
    }
}

class DataWriter implements ItemStreamWriter<Data> {

    @Override
    public void write(Chunk<? extends Data> chunk) throws Exception {
        chunk.getItems().forEach(e->{
            System.out.println(e.getValue());
        });
    }
}
