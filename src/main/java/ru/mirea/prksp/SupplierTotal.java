package ru.mirea.prksp;

import java.math.BigDecimal;

/** Накопительный итог по одному поставщику. */
public record SupplierTotal(
        int supplierId,
        String supplierName,
        int availableQuantity,
        BigDecimal totalCost
) {
}
