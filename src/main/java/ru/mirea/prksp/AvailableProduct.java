package ru.mirea.prksp;

import java.math.BigDecimal;

/** Проверенный товар с рассчитанной стоимостью доступного количества. */
public record AvailableProduct(
        int supplierId,
        String supplierName,
        String product,
        int quantity,
        BigDecimal totalCost
) {
}
