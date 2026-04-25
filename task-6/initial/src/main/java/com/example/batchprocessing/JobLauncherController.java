package com.example.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
public class JobLauncherController {

    private static final Logger log = LoggerFactory.getLogger(JobLauncherController.class);

    private final JobLauncher jobLauncher;
    private final Job importProductJob;

    public JobLauncherController(JobLauncher jobLauncher, Job importProductJob) {
        this.jobLauncher = jobLauncher;
        this.importProductJob = importProductJob;
    }

    @PostMapping("/import-products")
    public ResponseEntity<Map<String, Object>> run(
            @RequestParam(defaultValue = "product-data.csv") String fileName) throws Exception {

        log.info("Received request to start ETL job, file={}", fileName);

        JobParameters params = new JobParametersBuilder()
                .addString("fileName", fileName)
                .addLong("runId", System.currentTimeMillis())
                .toJobParameters();

        JobExecution execution = jobLauncher.run(importProductJob, params);

        log.info("Job submitted: executionId={}, status={}",
                execution.getId(), execution.getStatus());

        return ResponseEntity.ok(Map.of(
                "executionId", execution.getId(),
                "status", execution.getStatus().toString(),
                "fileName", fileName
        ));
    }
}