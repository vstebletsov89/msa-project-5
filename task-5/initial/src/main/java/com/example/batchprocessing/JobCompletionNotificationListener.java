package com.example.batchprocessing;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.prometheus.metrics.exporter.pushgateway.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	private final JdbcTemplate jdbcTemplate;
	private final MeterRegistry meterRegistry;
	private final String pushgatewayUrl;
	private final PrometheusMeterRegistry prometheusMeterRegistry;
	private final String pushgatewayUrl;

	private final AtomicLong lastRunStatus = new AtomicLong(1);
	private final AtomicLong lastSuccessTimestampSeconds = new AtomicLong(0);
	private final AtomicLong lastRunDurationSeconds = new AtomicLong(0);
	private final AtomicLong lastRunSkipCount = new AtomicLong(0);

	public JobCompletionNotificationListener(
			JdbcTemplate jdbcTemplate,
			MeterRegistry meterRegistry,
			PrometheusMeterRegistry prometheusMeterRegistry,
			@Value("${pushgateway.url}") String pushgatewayUrl
	) {
		this.jdbcTemplate = jdbcTemplate;
		this.prometheusMeterRegistry = prometheusMeterRegistry;
		this.pushgatewayUrl = pushgatewayUrl;

		Gauge.builder("stock_etl_last_run_status", lastRunStatus, AtomicLong::get)
				.description("Last Stock ETL run status: 1 = success, 0 = failed")
				.tag("application", "stock-etl-batch")
				.register(meterRegistry);

		Gauge.builder("stock_etl_last_success_timestamp_seconds", lastSuccessTimestampSeconds, AtomicLong::get)
				.description("Unix timestamp of the last successful Stock ETL run")
				.tag("application", "stock-etl-batch")
				.register(meterRegistry);

		Gauge.builder("stock_etl_last_run_duration_seconds", lastRunDurationSeconds, AtomicLong::get)
				.description("Duration of the last Stock ETL run in seconds")
				.tag("application", "stock-etl-batch")
				.register(meterRegistry);

		Gauge.builder("stock_etl_last_run_skip_count", lastRunSkipCount, AtomicLong::get)
				.description("Skipped records count in the last Stock ETL run")
				.tag("application", "stock-etl-batch")
				.register(meterRegistry);
	}

	@Override
	public void beforeJob(JobExecution jobExecution) {
		log.info("Batch job started: jobName={}, jobId={}, parameters={}",
				jobExecution.getJobInstance().getJobName(),
				jobExecution.getJobId(),
				jobExecution.getJobParameters());
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		boolean success = jobExecution.getStatus() == BatchStatus.COMPLETED;

		lastRunStatus.set(success ? 1 : 0);

		if (success) {
			lastSuccessTimestampSeconds.set(System.currentTimeMillis() / 1000);
		}

		if (jobExecution.getStartTime() != null && jobExecution.getEndTime() != null) {
			lastRunDurationSeconds.set(
					Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).getSeconds()
			);
		}

		long skipCount = jobExecution.getStepExecutions()
				.stream()
				.mapToLong(stepExecution -> stepExecution.getSkipCount())
				.sum();

		lastRunSkipCount.set(skipCount);

		if (success) {
			log.info("Batch job finished: jobName={}, jobId={}, status={}, durationSeconds={}, skipCount={}",
					jobExecution.getJobInstance().getJobName(),
					jobExecution.getJobId(),
					jobExecution.getStatus(),
					lastRunDurationSeconds.get(),
					lastRunSkipCount.get());

			jdbcTemplate.query(
					"SELECT productId, productSku, productName, productAmount, productData FROM products ORDER BY productId",
					new DataClassRowMapper<>(Product.class)
			).forEach(p -> log.info("Found product in database: {}", p));
		} else {
			log.error("Batch job failed: jobName={}, jobId={}, status={}, durationSeconds={}, skipCount={}",
					jobExecution.getJobInstance().getJobName(),
					jobExecution.getJobId(),
					jobExecution.getStatus(),
					lastRunDurationSeconds.get(),
					lastRunSkipCount.get());

			jobExecution.getAllFailureExceptions()
					.forEach(ex -> log.error("Batch job failure", ex));
		}

		pushMetricsToPushgateway(jobExecution);
	}

	private void pushMetricsToPushgateway(JobExecution jobExecution) {
		String jobName = jobExecution.getJobInstance().getJobName();
		String pushUrl = "%s/metrics/job/stock-etl-batch/batch_job/%s"
				.formatted(pushgatewayUrl, jobName);

		try {
			String metrics = prometheusMeterRegistry.scrape();

			HttpURLConnection connection = (HttpURLConnection) URI.create(pushUrl).toURL().openConnection();
			connection.setRequestMethod("PUT");
			connection.setDoOutput(true);
			connection.setRequestProperty("Content-Type", "text/plain; version=0.0.4; charset=utf-8");

			try (OutputStream os = connection.getOutputStream()) {
				os.write(metrics.getBytes(StandardCharsets.UTF_8));
			}

			int responseCode = connection.getResponseCode();

			if (responseCode >= 200 && responseCode < 300) {
				log.info("Pushed Stock ETL metrics to Pushgateway: url={}, responseCode={}", pushUrl, responseCode);
			} else {
				log.error("Failed to push Stock ETL metrics to Pushgateway: url={}, responseCode={}", pushUrl, responseCode);
			}

			connection.disconnect();
		} catch (Exception e) {
			log.error("Failed to push Stock ETL metrics to Pushgateway: url={}", pushUrl, e);
		}
	}
}