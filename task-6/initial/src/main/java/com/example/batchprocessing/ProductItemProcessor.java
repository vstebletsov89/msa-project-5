package com.example.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class ProductItemProcessor implements ItemProcessor<Product, Product> {

	private static final Logger log = LoggerFactory.getLogger(ProductItemProcessor.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Override
	public Product process(final Product product) {
		String loyality = jdbcTemplate.query(
				"SELECT loyalityData FROM loyality_data WHERE productSku = ?",
				ps -> ps.setLong(1, product.productSku()),
				rs -> rs.next() ? rs.getString(1) : null
		);

		String enriched = loyality != null ? loyality : product.productData();

		Product result = new Product(
				product.productId(),
				product.productSku(),
				product.productName(),
				product.productAmount(),
				enriched
		);

		log.info("Processing product sku={} : '{}' -> '{}'",
				product.productSku(), product.productData(), enriched);

		return result;
	}
}