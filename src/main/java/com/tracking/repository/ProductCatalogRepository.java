package com.tracking.repository;

import com.tracking.model.Product;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class ProductCatalogRepository {
    private final Map<String, Product> productsById = new ConcurrentHashMap<>();
    private final Map<String, List<Product>> productsByCategory = new ConcurrentHashMap<>();
    private final Map<String, List<Product>> productsByBrand = new ConcurrentHashMap<>();

    public ProductCatalogRepository() {
        loadCatalog();
    }

    private void loadCatalog() {
        try (InputStream is = getClass().getResourceAsStream("/products.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
             
            if (is == null) {
                log.error("products.csv not found in classpath");
                return;
            }

            // Skip header
            String line = reader.readLine();
            
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 3) {
                    Product product = new Product(parts[0].trim(), parts[1].trim(), parts[2].trim());
                    
                    productsById.put(product.getProductId(), product);
                    productsByCategory.computeIfAbsent(product.getCategoryId(), k -> new ArrayList<>()).add(product);
                    productsByBrand.computeIfAbsent(product.getBrandId(), k -> new ArrayList<>()).add(product);
                }
            }
            log.info("Loaded {} products into catalog", productsById.size());
        } catch (Exception e) {
            log.error("Failed to load products.csv", e);
        }
    }

    public Product getProduct(String productId) {
        return productsById.get(productId);
    }

    public List<Product> getRecommendations(String productId, int limit) {
        Product targetProduct = productsById.get(productId);
        if (targetProduct == null) {
            return Collections.emptyList();
        }

        List<Product> recommendations = new ArrayList<>();
        
        // 1. Same category
        List<Product> sameCategory = productsByCategory.getOrDefault(targetProduct.getCategoryId(), Collections.emptyList());
        for (Product p : sameCategory) {
            if (!p.getProductId().equals(productId) && !recommendations.contains(p)) {
                recommendations.add(p);
                if (recommendations.size() >= limit) return recommendations;
            }
        }
        
        // 2. Same brand (if needed to fill limit)
        List<Product> sameBrand = productsByBrand.getOrDefault(targetProduct.getBrandId(), Collections.emptyList());
        for (Product p : sameBrand) {
            if (!p.getProductId().equals(productId) && !recommendations.contains(p)) {
                recommendations.add(p);
                if (recommendations.size() >= limit) return recommendations;
            }
        }
        
        return recommendations;
    }
}
