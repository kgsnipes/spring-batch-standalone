package org.example;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {
    public static void main(String[] args)throws Exception {
        // Creating the application context object
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.scan("org.example.*");
        context.refresh();
        // Creating the job launcher
        JobLauncher jobLauncher = (JobLauncher) context.getBean("jobLauncher");
        // Creating the job
        Job job = (Job) context.getBean("printJob");
        // Executing the JOB
        JobExecution execution = jobLauncher.run(job, new JobParameters());
        System.out.println("Exit Status : " + execution.getStatus());

}
}